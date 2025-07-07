#!/bin/python
"""
LTC Hub Build Framework - Generic Implementation

This framework provides a configuration-driven approach to building hub tables following the CARA pattern.
Instead of creating separate Python files for each hub table, this framework uses dictionary (.dic) files
to configure source tables, joins, and field transformations.

Features:
- Single, generic implementation for all hub tables
- Modular, function-based design with clear separation of concerns
- Support for multiple driving tables and complex join patterns
- Dictionary-driven configuration with no hardcoded values
- Robust error handling and logging
- SCD Type 2 versioning support
- Backward compatibility with existing implementations

Usage:
1. Create a dictionary (.dic) file with source tables and transformations
2. Configure YAML file to point to the dictionary
3. Call process_hub_table(spark, props, app_logger) from driver.py

For detailed documentation, see LTC_FW_Instructions.md
"""

import json
import sys
import os
from datetime import datetime

sys.dont_write_bytecode = True
from pyspark.sql.functions import *
from src.run.custom_logging import *
from src.utils.df_utils import create_sha, date_time_now, selective_df_custom, save_hive
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DateType, DecimalType, TimestampType
from pyspark.sql.functions import broadcast, coalesce, desc, lit, rank
from operator import itemgetter

# ============================================================================
# UTILITY AND VALIDATION FUNCTIONS
# ============================================================================

def validate_dictionary_structure(table_columns, app_logger):
    """
    Validates that the dictionary has the required structure and components.
    
    Args:
        table_columns: Dictionary configuration
        app_logger: Application logger
        
    Returns:
        bool: True if valid, False otherwise
    """
    try:
        # Check for transformations section
        if "transformations" not in table_columns:
            app_logger.error("Missing 'transformations' section in dictionary")
            return False
            
        # Check for fields section
        transformations = table_columns["transformations"]
        if "fields" not in transformations:
            app_logger.error("Missing 'fields' section in dictionary")
            return False
            
        # Check for at least one source table
        source_tables = [k for k in table_columns.keys() if k != "transformations"]
        if not source_tables:
            app_logger.error("No source tables defined in dictionary")
            return False
            
        # For tables with complex expressions, check for function replacements
        fields = transformations.get("fields", {})
        complex_expr_keywords = transformations.get("complex_expr_keywords", [])
        
        if complex_expr_keywords:
            # Complex expressions are used, check if function_replacements is defined
            if "function_replacements" not in transformations:
                app_logger.warning("Complex expressions are used but no function_replacements defined")
                # Not a critical error, just a warning
        
        return True
        
    except Exception as e:
        app_logger.error(f"Error validating dictionary structure: {str(e)}")
        return False

def get_table_statistics(df_dict, app_logger):
    """
    Gets basic statistics about loaded tables without triggering Spark actions.
    
    Args:
        df_dict: Dictionary of dataframes
        app_logger: Application logger
        
    Returns:
        dict: Dictionary with table stats
    """
    stats = {}
    for name, df in df_dict.items():
        if df is not None:
            table_name = name.replace("df_", "")
            stats[table_name] = {
                "columns": len(df.columns),
                "column_names": df.columns
            }
    
    app_logger.info(f"Loaded {len(stats)} tables with data")
    return stats

def parse_case_statement(expr, app_logger):
    """
    Parse SQL CASE statements into PySpark when/otherwise expressions.
    
    Args:
        expr: SQL CASE statement as string
        app_logger: Application logger
        
    Returns:
        string: PySpark expression ready for evaluation
    """
    try:
        case_parts = expr.upper().split('CASE')[1].split('END')[0].strip()
        
        # Extract the column being compared if this is a simple CASE
        if case_parts.startswith('WHEN'):
            # This is a searched CASE expression (WHEN condition THEN result)
            when_parts = case_parts.split('WHEN')[1:]
            
            # Extract all WHEN/THEN pairs
            when_blocks = []
            for part in when_parts:
                if 'THEN' in part:
                    condition, result = part.split('THEN', 1)
                    # Clean up and prepare for PySpark conversion
                    condition = condition.strip()
                    result = result.split('WHEN')[0].strip() if 'WHEN' in result else result.strip()
                    
                    # Convert to PySpark format with appropriate replacements
                    condition = condition.replace("=", "==").replace(" AND ", " & ").replace(" OR ", " | ")
                    
                    # Handle string literals in results
                    if result.startswith("'") and result.endswith("'"):
                        result = f"\"{result[1:-1]}\""
                    
                    when_blocks.append((condition, result))
            
            # Extract ELSE clause if present
            else_result = None
            if 'ELSE' in case_parts:
                else_parts = case_parts.split('ELSE')[1].strip()
                else_result = else_parts.split('WHEN')[0].strip() if 'WHEN' in else_parts else else_parts
                if else_result.startswith("'") and else_result.endswith("'"):
                    else_result = f"\"{else_result[1:-1]}\""
            
            # Build PySpark when/otherwise chain
            when_expr = []
            for condition, result in when_blocks:
                when_expr.append(f"when({condition}, {result})")
            
            # Combine all WHEN clauses
            if when_expr:
                expr = ".".join(when_expr)
                if else_result:
                    expr += f".otherwise({else_result})"
                    
            app_logger.info(f"Successfully parsed CASE statement: {expr}")
            return expr
        
        return expr
            
    except Exception as e:
        app_logger.warning(f"Failed to parse CASE statement: {str(e)}")
        app_logger.warning(f"Returning original expression: {expr}")
        return expr

# ============================================================================
# DATA LOADING FUNCTIONS
# ============================================================================

def load_source_tables(spark, source_db, table_columns, active_dt, app_logger):
    """
    Load all source tables defined in the dictionary configuration.
    
    Args:
        spark: Spark session object
        source_db: Source database name
        table_columns: Dictionary configuration from .dic file
        active_dt: Active date for filtering
        app_logger: Application logger
        
    Returns:
        dict: Dictionary of dataframes with keys 'df_{table_name}'
    """
    app_logger.info("Loading source tables from dictionary configuration")
    df_dict = {'df_' + k: None for k in table_columns.keys() if k != "transformations"}
    
    for db_table in table_columns.keys():
        if db_table == "transformations":
            continue
            
        try:
            # Use selective_df_custom directly without exec for better readability and maintainability
            df = selective_df_custom(
                spark,
                f"{source_db}.{db_table}",
                table_columns[db_table],
                active_dt,
                table_columns[db_table].get('part_fields')
            )
            # Store the dataframe in the dictionary
            df_dict[f"df_{db_table}"] = df
            app_logger.debug(f"Processed {db_table}")
            app_logger.info(f"Successfully loaded source table {db_table}")
        except Exception as e:
            app_logger.error(f"Failed to load source table {db_table}: {str(e)}")
            print(f"ERROR: Source table load failed for {db_table}: {str(e)}")
            raise
    
    return df_dict

# ============================================================================
# JOIN AND TRANSFORMATION FUNCTIONS
# ============================================================================

def apply_join(base_df, join_df, condition_str, join_type, app_logger):
    """
    Apply a join between two dataframes using the specified condition.
    
    Args:
        base_df: Base dataframe
        join_df: Dataframe to join
        condition_str: Join condition as string
        join_type: Type of join (inner, left, right, full)
        app_logger: Application logger
        
    Returns:
        DataFrame: Joined dataframe
    """
    try:
        # Convert SQL-style conditions to PySpark expressions
        condition_expr = condition_str.replace('=', '==').replace(' AND ', ' & ').replace(' OR ', ' | ')
        
        # Build the join condition expression
        join_condition = eval(f"({condition_expr})")
        
        # Apply the join
        result_df = base_df.join(join_df, join_condition, join_type)
        app_logger.info(f"Successfully applied {join_type} join with condition: {condition_str}")
        
        return result_df
        
    except Exception as e:
        app_logger.error(f"Error applying join with condition '{condition_str}': {str(e)}")
        raise

def build_joined_dataframe(df_dict, table_columns, app_logger):
    """
    Build and join dataframes based on configuration from dictionary.
    Supports multiple driving tables through the 'driving_tables' configuration.
    
    Args:
        df_dict: Dictionary of dataframes
        table_columns: Dictionary configuration from .dic file
        app_logger: Application logger
        
    Returns:
        DataFrame: Joined dataframe according to configuration
    """
    app_logger.info("Building joined dataframe from configuration")
    
    try:
        # Get transformation configurations from dictionary
        transforms = table_columns.get("transformations", {})
        joins = transforms.get("joins", [])
        
        # Get driving tables configuration - supports both single and multiple driving tables
        driving_tables = transforms.get("driving_tables", [])
        
        # Handle single driving table for backward compatibility
        if not driving_tables:
            driving_table = transforms.get("driving_table")
            if driving_table:
                driving_tables = [driving_table]
            else:
                # Default to first table in dictionary if not specified
                driving_tables = [next((k for k in table_columns.keys() if k != "transformations"), None)]
                
        if not driving_tables:
            raise ValueError("No driving tables specified or found in dictionary")
            
        # Start with first driving table as base
        primary_table = driving_tables[0]
        app_logger.info(f"Using primary driving table: {primary_table}")
        
        # Verify driving table exists
        driving_df_key = f"df_{primary_table}"
        if driving_df_key not in df_dict or df_dict[driving_df_key] is None:
            raise ValueError(f"Primary driving table '{primary_table}' not found in loaded dataframes")
        
        # Start with first driving table
        final_data = df_dict[driving_df_key].alias(primary_table)
        
        # If multiple driving tables, join them first
        if len(driving_tables) > 1:
            app_logger.info(f"Multiple driving tables detected: {driving_tables}")
            
            # Process additional driving tables with special joins
            for idx, table in enumerate(driving_tables[1:], 1):
                table_df_key = f"df_{table}"
                if table_df_key not in df_dict or df_dict[table_df_key] is None:
                    app_logger.warning(f"Secondary driving table '{table}' not found, skipping")
                    continue
                    
                # Look for special join configuration for this driving table
                driving_join = next((j for j in joins if j.get("table") == table and j.get("is_driving", False)), None)
                
                if driving_join:
                    join_type = driving_join.get("type", "inner")  # Default to inner for driving tables
                    condition = driving_join.get("condition")
                    
                    # Apply join with appropriate condition
                    final_data = apply_join(
                        final_data, 
                        df_dict[table_df_key].alias(table), 
                        condition, 
                        join_type, 
                        app_logger
                    )
                else:
                    app_logger.warning(f"No join configuration found for driving table {table}, using natural join")
                    # Apply default join (inner join on matching columns)
                    common_cols = set(final_data.columns).intersection(df_dict[table_df_key].columns)
                    if common_cols:
                        join_cols = [col(f"{primary_table}.{c}") == col(f"{table}.{c}") for c in common_cols]
                        join_condition = reduce(lambda x, y: x & y, join_cols)
                        final_data = final_data.join(df_dict[table_df_key].alias(table), join_condition, "inner")
                    else:
                        app_logger.error(f"Cannot join driving tables - no common columns found")
                        raise ValueError(f"Cannot join driving tables - no common columns")
        
        # Apply remaining joins from dictionary configuration
        for join_config in joins:
            # Skip joins for driving tables as they're handled separately
            table_name = join_config.get("table")
            if table_name in driving_tables and join_config.get("is_driving", False):
                continue
                
            join_type = join_config.get("type", "left")
            condition = join_config.get("condition")
            
            if table_name and condition:
                # Get the dataframe for this table
                table_df_key = f"df_{table_name}"
                if table_df_key in df_dict and df_dict[table_df_key] is not None:
                    # Apply join based on configuration
                    final_data = apply_join(
                        final_data, 
                        df_dict[table_df_key].alias(table_name), 
                        condition, 
                        join_type, 
                        app_logger
                    )
                else:
                    app_logger.warning(f"Table {table_name} not found in loaded dataframes, skipping join")
            else:
                app_logger.warning(f"Invalid join configuration detected: {join_config}, skipping join")
        
        return final_data
        
    except Exception as e:
        app_logger.error(f"Failed to build joined dataframe: {str(e)}")
        raise

def apply_field_transformations(joined_df, table_columns, app_logger, props=None):
    """
    Apply field transformations from dictionary configuration.
    
    Args:
        joined_df: Joined dataframe
        table_columns: Dictionary configuration from .dic file
        app_logger: Application logger
        props: Optional properties dictionary for error messages
        
    Returns:
        DataFrame: Transformed dataframe with all fields applied
    """
    app_logger.info("Applying field transformations from dictionary")
    
    try:
        # Get transformation configurations from dictionary
        transforms = table_columns.get("transformations", {})
        fields_config = transforms.get("fields", {})
        select_expr = []
        
        # Process each target field defined in dictionary
        for target_field, source_expr in fields_config.items():
            try:
                # Get complex expression keywords from dictionary
                complex_expr_keywords = transforms.get("complex_expr_keywords", [])
                
                # Check if complex expression keywords are defined
                if not complex_expr_keywords:
                    app_logger.warning("No complex_expr_keywords defined in dictionary.")
                    # Use default keywords as fallback
                    complex_expr_keywords = transforms.get("default_complex_keywords", 
                        ["CASE", "CAST", "TO_DATE", "CURRENT_DATE", "CONCAT_WS", "COALESCE"])
                    app_logger.warning(f"Using default complex expression keywords: {complex_expr_keywords}")
                
                # Check if this field contains any complex expressions
                if any(keyword in source_expr.upper() for keyword in complex_expr_keywords):
                    # Get function replacements from dictionary
                    replacements = transforms.get("function_replacements", {})
                    if not replacements:
                        app_logger.error("No function_replacements defined in dictionary.")
                        if props:
                            app_logger.error(f"Please update the dictionary file '{props.get('CONFIG_DIC_FILE_NAME', '')}' "
                                           "to include function_replacements section.")
                        replacements = {}  # Use empty dict as fallback
                    
                    # Apply replacements to convert SQL to PySpark syntax
                    expr = source_expr
                    for sql_syntax, spark_syntax in replacements.items():
                        expr = expr.replace(sql_syntax, spark_syntax)
                    
                    # Handle CASE statements with generic parser
                    if "CASE" in expr.upper():
                        expr = parse_case_statement(expr, app_logger)
                    
                    # Evaluate the transformed expression
                    select_expr.append(eval(expr).alias(target_field))
                else:
                    # Simple column reference
                    if '.' in source_expr:
                        table, field = source_expr.split('.')
                        select_expr.append(col(f"{table}.{field}").alias(target_field))
                    else:
                        # Handle direct column references without table prefix
                        select_expr.append(col(source_expr).alias(target_field))
                        
            except Exception as e:
                app_logger.error(f"Error processing field {target_field}: {str(e)}")
                # Add a null column as fallback
                select_expr.append(lit(None).cast("string").alias(target_field))
        
        # Apply the select expression to create final dataframe
        return joined_df.select(*select_expr)
        
    except Exception as e:
        app_logger.error(f"Failed to apply field transformations: {str(e)}")
        raise

def apply_scd_type2(final_data, table_columns, props, spark, active_dt, source_type, target_db, target_table, app_logger):
    """
    Apply SCD Type 2 versioning to the final dataframe.
    
    Args:
        final_data: Transformed dataframe
        table_columns: Dictionary configuration from .dic file
        props: Dictionary of properties from YAML
        spark: Spark session object
        active_dt: Active date
        source_type: Source system identifier
        target_db: Target database
        target_table: Target table
        app_logger: Application logger
        
    Returns:
        DataFrame: DataFrame with SCD Type 2 versioning applied
    """
    try:
        # Define primary key - use business keys from dictionary
        primary_key_cols = table_columns.get("transformations", {}).get("primary_key", ["acc_no"])
        
        # Get current timestamp for record creation/modification
        dt_now = date_time_now()
        active_ym = date_format(to_date(lit(active_dt)), "yyyy-MM")
        
        # Create SHA for change detection using create_sha instead of create_sha_field directly
        # This excludes timestamp fields from SHA calculation for better change detection
        final_data = create_sha(final_data, ['_timestamp'])
        
        # Add metadata fields
        final_data = (final_data
            .withColumn("active_dt", lit(active_dt))
            .withColumn("active_ym", active_ym)
            .withColumn("load_dt", lit(dt_now))
            .withColumn("source", lit(source_type))
        )
        
        # Check if this is a first-time load
        if props.get("load_type", "").upper() == "HISTORY":
            # For history load, simply set rec_version to 1
            app_logger.info("History load detected - creating initial versions with rec_version=1")
            final_data = final_data.withColumn("rec_version", lit(1))
        else:
            # Normal incremental load - check for existing data
            target_table_exists = spark.catalog.tableExists(target_db, target_table)
            
            if not target_table_exists:
                app_logger.info("Target table doesn't exist - creating initial versions with rec_version=1")
                final_data = final_data.withColumn("rec_version", lit(1))
            else:
                app_logger.info("Incremental load - merging with history")
                
                # Define window functions for entity/granularity keys
                w = Window.partitionBy(*primary_key_cols).orderBy(desc("active_dt"), desc("rec_version"))
                
                # Get the latest record for each entity to compare
                history_data = (spark.table(f"{target_db}.{target_table}")
                    .filter((col("source") == source_type) & (col("active_dt") <= lit(active_dt)))
                    .select(*primary_key_cols, "rec_sha", "rec_version", "active_dt")
                    .withColumn("rnk", rank().over(w))
                    .filter(col("rnk") == 1)
                    .select(
                        *[col(key).alias(f"src_{key}") for key in primary_key_cols],
                        col("rec_sha").alias("src_rec_sha"),
                        col("rec_version")
                    )
                    .cache()
                )

                # Anti-join to find records that have changed or are new
                scd_data = final_data.alias("n").join(
                    history_data.alias("h"),
                    col("h.src_rec_sha") == col("n.rec_sha"),
                    "left_anti"
                )
                
                # Join with history to get the latest version number
                join_cols = [
                    col(f"h.src_{key}") == col(f"n.{key}") for key in primary_key_cols
                ]
                
                # Create new version numbers based on history
                if join_cols:
                    join_condition = reduce(lambda x, y: x & y, join_cols)
                    final_data = (scd_data.alias("n")
                        .join(history_data.alias("h"), join_condition, "left_outer")
                        .select(
                            *[col("n." + c) for c in final_data.columns],
                            (coalesce(col("h.rec_version"), lit(0)) + lit(1)).alias("rec_version")
                        )
                    )
                else:
                    # Handle case with no primary keys defined
                    app_logger.warning("No primary keys defined for SCD Type 2 versioning - using default version 1")
                    final_data = scd_data.withColumn("rec_version", lit(1))
                
                # Clean up cache
                history_data.unpersist()
        
        app_logger.info("Successfully applied SCD Type 2 versioning")
        return final_data
        
    except Exception as e:
        app_logger.error(f"Failed to apply SCD Type 2 versioning: {str(e)}")
        raise

# ============================================================================
# MAIN ORCHESTRATION FUNCTIONS
# ============================================================================

def process_hub_table_generic(spark, props, app_logger):
    """
    Transform source data into hub table format following CARA pattern.
    This is the main generic function for the LTC Framework.
    
    This implementation follows the CARA pattern requirements as a generic framework:
    - Uses dictionary-based configuration for source loading
    - Implements SCD Type 2 versioning with rec_version
    - Uses standard _timestamp field (not entity-specific timestamps) for versioning
    - Uses the df_utils.py functions for shared functionality (create_sha)
    - Saves data to Hive with the required save_hive function
    
    Configuration-driven features in dictionary (.dic) file:
    - Table selection and column mapping
    - Join configuration with tables and conditions
    - Field transformation mappings
    - SQL-to-PySpark function replacements for SQL syntax translation
    - Primary key and business key definitions
    - Multiple driving tables support
    
    Dictionary structure:
    {
        "table1": { 
            "part_fields": "...",
            "sel_fields": "...",
            ...
        },
        "transformations": {
            "driving_tables": ["table1", "table2"],  # Multiple driving tables support
            "joins": [
                {
                    "table": "table2",
                    "condition": "table1.id = table2.id",
                    "is_driving": true  # Flag for driving table join
                },
                {
                    "table": "table3",
                    "condition": "table1.id = table3.id"
                }
            ],
            "fields": {...},
            "primary_key": [...],
            "business_keys": [...],
            "function_replacements": {  # REQUIRED
                "CAST(": "cast(",
                " AS ": ".cast(",
                ...
            },
            "complex_expr_keywords": [  # REQUIRED
                "CASE", "CAST", "CONCAT_WS", "YEAR", ...
            ],
            "default_complex_keywords": [  # OPTIONAL
                "CASE", "CAST", "CONCAT_WS", "YEAR", ...
            ]
        }
    }
    
    Note: If your fields contain complex expressions (CASE statements, functions, etc.), 
    you MUST define both "function_replacements" and "complex_expr_keywords" in the dictionary file.
    
    - "complex_expr_keywords" tells the program which expressions need special processing
    - "function_replacements" defines how SQL syntax is converted to PySpark syntax
    - "default_complex_keywords" provides fallback keywords if complex_expr_keywords is not defined
      If you only have simple column references, complex_expr_keywords can be an empty list.
    
    Args:
        spark: Spark session object
        props: Dictionary of properties from YAML configuration
        app_logger: Application logger object
        
    Returns:
        DataFrame: Final transformed dataframe that is also saved to Hive
    """
    print(f"In {__name__} module run function")
    app_logger.info(f"Started processing for {props['TARGET_TBL']}")
    
    # Extract configuration properties
    target_table = props['TARGET_TBL']
    source_type = props['SOURCE_TYPE'].upper()
    source_db = props['SOURCE_DB'].strip()
    target_db = props['TARGET_DB'].strip()
    active_dt = props.get('active_dt')
    
    try:
        # Step 1: Load dictionary configuration
        dic_file_path = os.path.join(os.getcwd(), props['CONFIG_DIC_FILE_NAME'])
        app_logger.info(f"Loading dictionary from {dic_file_path}")
        with open(dic_file_path, 'r') as f:
            table_columns = json.load(f)
        
        # Validate dictionary structure
        if not validate_dictionary_structure(table_columns, app_logger):
            app_logger.error(f"Dictionary structure validation failed for {props['CONFIG_DIC_FILE_NAME']}")
            print(f"ERROR: Dictionary structure validation failed - check logs for details")
            sys.exit(1)
        
        # Step 2: Load source tables 
        df_dict = load_source_tables(spark, source_db, table_columns, active_dt, app_logger)
        
        # Get statistics about loaded tables
        get_table_statistics(df_dict, app_logger)
        
        # Step 3: Build joined dataframe
        joined_df = build_joined_dataframe(df_dict, table_columns, app_logger)
        
        # Step 4: Apply field transformations
        transformed_df = apply_field_transformations(joined_df, table_columns, app_logger, props)
        
        # Step 5: Apply SCD Type 2 versioning
        final_data = apply_scd_type2(transformed_df, table_columns, props, spark, 
                                    active_dt, source_type, target_db, target_table, app_logger)
        
        # Step 6: Save data to Hive
        app_logger.info("Saving final data to Hive")
        save_hive(spark=spark, 
                 df=final_data, 
                 columns_obj=props, 
                 hive_db_name=target_db,
                 hive_table_name=target_table, 
                 app_logger=app_logger, 
                 mode="append_partition",                 partition_column="source,active_ym")
                 
        app_logger.info(f"Successfully saved data to {target_db}.{target_table}")
        return final_data
        
    except Exception as e:
        app_logger.error(f"Error in process_hub_table_generic: {str(e)}")
        print(f"ERROR: Process failed: {str(e)}")
        print("Oops!", sys.exc_info()[0], "occurred.")
        sys.exit(1)

# ============================================================================
# ENTRY POINT FUNCTIONS
# ============================================================================

def process_hub_table(spark, props, app_logger):
    """
    Generic entry point for any hub table using the LTC Framework.
    
    Args:
        spark: Spark session object
        props: Dictionary of properties from YAML configuration
        app_logger: Application logger object
        
    Returns:
        DataFrame: Final transformed dataframe
    """
    return process_hub_table_generic(spark, props, app_logger)

# Aliases for backward compatibility
hub_framework = process_hub_table
hub_pimsctp_accident = process_hub_table  # For legacy code that might still call this function
