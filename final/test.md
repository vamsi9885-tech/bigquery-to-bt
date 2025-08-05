Good morning/afternoon, everyone. I’ll walk you through the complete flow of our data processing pipeline, starting from the driver level down to the hub load. I’ll explain each key file, its role, and how data flows through the system.

Step 1: driver.py – The Entry Point

We start with the driver.py file. This is our main controller script that triggers the execution.

- It takes parameters from the command line like --env, --conf_file, --type_of_load, and others.
- Based on the type_of_load value, it decides which specific module to run.
- It loads configuration and storage YAML files.
- Then it calls the run() method of the module class. For example, in our case, it will call the HubPimsctpParticipant class from hub_participant.py.
- One key advantage is that we have a **single reusable module structure** for all hub tables. This means we don’t have to modify the driver.py file for every new table — we just need to create a new domain class that inherits the base and update the config if needed. This ensures better scalability and less maintenance.


Step 2: hub_participant.py – Business-Specific Logic

Next, we move to hub_participant.py. This script contains logic specific to the participant entity.

- It inherits from a base class called HubBase.
- It reads a YAML config file to fetch metadata about the tables it needs.
- Then it loads several participant source tables like dl_CTPParticipantVehiclePax, dl_CTPParticipantPedestrian, etc.
- It combines all those using union, adds a ROLE column to identify participant types, and performs multiple joins with lookup tables such as dl_CTPAccident, dl_CTPPoliceFinding, and dl_CTPDrugTest.
- This transformation logic happens inside the transform() method, and the result is passed back to the HubBase class for further processing.

Step 3: final_hubBase.py – Common ETL Logic

Now the final_hubBase.py file – this acts as the reusable ETL framework.

- It handles standard logic like deduplication, versioning, and partitioning.
- It first drops duplicate records and generates a SHA hash key called rec_sha.
- Then it adds required metadata columns such as active_dt, active_ym, load_dt, and source_system_cd.
- If the load_type is HISTORY, it assigns record_version_no as 1 directly.
- If not, it compares the incoming records with historical records to check for changes using the SHA key. If changes are found, it increments the version number.
- Finally, it writes the result to Hive using a method called save_hive_overwrite().

Step 4: df_utils.py – Utility Functions

We also use a helper module called df_utils.py which contains reusable utility functions like:

- create_sha_field() to generate a unique hash.
- save_hive_overwrite() to write data to Hive with partitioning.
- selective_df_custom() to read and filter Hive data with ranking logic.
- These utilities improve maintainability and avoid repeating the same code across multiple modules.

Step 5: hub_pimsctp_participant.dic – Config Driven

Lastly, the .dic file (hub_pimsctp_participant.dic) is a dictionary-style YAML config file.

- It contains metadata like sel_fields, part_fields, and rnk_fields.
- This config is used inside hub_participant.py to read only required columns, apply filters, and implement ranking logic.

Flow Summary – End-to-End Recap

To summarize the full flow:

1. driver.py kicks off the pipeline with arguments.
2. It loads YAML configurations for both storage and processing.
3. Then it triggers the domain-specific module like hub_participant.py.
4. That module uses dictionary-based configs and joins all required tables.
5. The processed DataFrame is passed to final_hubBase.py where SCD versioning and partition logic are applied.
6. Finally, the result is saved to Hive in a clean, version-controlled format.

Closing Statement

This flow is modular, YAML-driven, and scalable for onboarding new data sources into our hub layer. It maintains consistency, reusability, and handles versioning out-of-the-box.

Let me know if you'd like me to walk through the codebase or explain any specific file in more detail.
