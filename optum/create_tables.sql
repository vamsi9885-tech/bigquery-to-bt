CREATE TABLE IF NOT EXISTS public.cadence_details (
	frequency text NOT NULL,
	entry_id int4 NOT NULL,
	time_interval_in_days int4 NOT NULL,
	created_time_stamp timestamp NULL DEFAULT CURRENT_TIMESTAMP,
	modified_by text NOT NULL DEFAULT 'build'::text,
	created_by text NOT NULL DEFAULT 'build'::text
);
----------
CREATE TABLE IF NOT EXISTS public.client_feed_config (
	client_id text NOT NULL,
	client_name text NOT NULL,
	feed_name text NOT NULL,
	feed_config json NULL,
	modified_by text NOT NULL DEFAULT 'build'::text,
	client_short_name text NOT NULL,
	created_by text NOT NULL DEFAULT 'build'::text,
	created_time_stamp timestamp NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT client_feed_config_pk PRIMARY KEY (feed_name)
);
----------
CREATE TABLE IF NOT EXISTS public.feed_master (
	feed_id text NOT NULL,
	feed_name text NOT NULL,
	adf_run_id text NOT NULL,
	landed_file_name text NULL,
	is_unzip_success bool NULL,
	is_adf_success bool NULL,
	is_landed bool NULL,
	extracted_count_of_files int8 NULL DEFAULT 0,
	error_source _text NULL DEFAULT '{}'::text[],
	error_message _text NULL DEFAULT '{}'::text[],
	created_time_stamp timestamp NULL,
	modified_by text NOT NULL DEFAULT 'adf'::text,
	created_by text NOT NULL DEFAULT 'adf'::text,
	CONSTRAINT feedmaster_pkey PRIMARY KEY (feed_id)
);
ALTER TABLE public.feed_master ADD CONSTRAINT feedmaster_fkey FOREIGN KEY (feed_name) REFERENCES public.client_feed_config(feed_name);
alter table public.feed_master alter column created_time_stamp set default current_timestamp;
----------
CREATE TABLE IF NOT EXISTS public.cadence_master (
	cadence_id text NOT NULL,
	feed_name text NULL,
	feed_startdate date NULL,
	feed_enddate date NULL,
	feed_type text NULL,
	cadence_completion_flag bool NULL,
	cadence_completion_status_sent_date date NULL,
	created_time_stamp timestamp NULL,
	modified_by text NOT NULL DEFAULT 'cadence_generator'::text,
	created_by text NOT NULL DEFAULT 'cade_generator'::text,
	CONSTRAINT "CadenceMaster_pkey" PRIMARY KEY (cadence_id)
);
ALTER TABLE public.cadence_master ADD CONSTRAINT cadencemaster_fkey FOREIGN KEY (feed_name) REFERENCES public.client_feed_config(feed_name);
ALTER TABLE public.cadence_master ADD COLUMN extraction_type text DEFAULT 'NONE';
alter table public.cadence_master alter column created_time_stamp set default current_timestamp;
alter table public.cadence_master alter column modified_by set default 'adf';
alter table public.cadence_master alter column created_by set default 'adf';
ALTER TABLE public.cadence_master ADD COLUMN output_folder_name text NULL;
----------
CREATE TABLE IF NOT EXISTS public.file_master (
	cadence_id text NOT NULL,
	feed_id text NULL,
	file_name text NULL,
	start_date date NULL,
	end_date date NULL,
	connector text NULL,
	extraction_script_name text NULL,
	logical_file_name text NULL,
	file_name_format text NOT NULL,
	source_row_count int8 NULL,
	processed_row_count int8 NULL,
	failed_row_count int8 NULL,
	is_schema_validation_success bool NULL,
	creation_time text NULL,
	filters text NULL,
	is_extraction_success bool NULL,
	file_size int8 NULL,
	is_adhoc_run bool NULL,
	status text NULL,
	notification_sent_date date NULL,
	delay_reason text NULL,
	disabled bool NULL,
	disabled_until date NULL,
	arrived_flag bool NULL,
	arrival_date date NULL,
	re_run_count int8 NULL DEFAULT 0,
	error_source _text NULL DEFAULT '{}'::text[],
	error_message _text NULL DEFAULT '{}'::text[],
	created_time_stamp timestamp NULL,
	modified_time_stamp timestamp NULL,
	modified_by text NOT NULL DEFAULT 'cadence_generator'::text,
	created_by text NOT NULL DEFAULT 'cade_generator'::text,
	CONSTRAINT file_master_pk PRIMARY KEY (cadence_id, file_name_format)
);
ALTER TABLE public.file_master ADD CONSTRAINT filemaster_fkey FOREIGN KEY (cadence_id) REFERENCES public.cadence_master(cadence_id);
ALTER TABLE public.file_master ADD CONSTRAINT filemaster_feedid_fkey FOREIGN KEY (feed_id) REFERENCES public.feed_master(feed_id);
ALTER TABLE public.file_master ADD COLUMN extraction_type text DEFAULT 'NONE';
ALTER TABLE public.file_master ADD COLUMN part int8 DEFAULT 1;
ALTER TABLE public.file_master DROP CONSTRAINT file_master_pk;
ALTER TABLE public.file_master ADD CONSTRAINT file_master_pk PRIMARY KEY (cadence_id, extraction_type, file_name_format, part);
ALTER TABLE public.file_master RENAME COLUMN is_schema_validation_success TO is_type_casting_success;
ALTER TABLE public.file_master ADD COLUMN query_start_date date NULL;
ALTER TABLE public.file_master ADD COLUMN query_end_date date NULL;
alter table public.file_master alter column created_time_stamp set default current_timestamp;
alter table public.file_master alter column modified_by set default 'adf';
alter table public.file_master alter column created_by set default 'adf';
ALTER TABLE public.file_master ADD COLUMN split_by text NULL;
ALTER TABLE public.file_master ADD COLUMN adhoc_start_date date NULL;
ALTER TABLE public.file_master ADD COLUMN adhoc_end_date date NULL;
----------
insert into public.cadence_details (frequency,entry_id,time_interval_in_days,created_time_stamp,created_by,modified_by) values('yearly','1','365',CURRENT_TIMESTAMP,'build','build');
----------------------
do $$
begin
for r in 1..12 loop
insert into public.cadence_details (frequency,entry_id,time_interval_in_days,created_time_stamp,created_by,modified_by) values('monthly',r,'30',CURRENT_TIMESTAMP,'build','build');
end loop;
end;
$$;
----------------------
do $$
begin
for r in 1..52 loop
insert into public.cadence_details (frequency,entry_id,time_interval_in_days,created_time_stamp,created_by,modified_by) values('weekly',r,'7',CURRENT_TIMESTAMP,'build','build');
end loop;
end;
$$;
----------------------
do $$
begin
for r in 1..26 loop
insert into public.cadence_details (frequency,entry_id,time_interval_in_days,created_time_stamp,created_by,modified_by) values('biweekly',r,'14',CURRENT_TIMESTAMP,'build','build');
end loop;
end;
$$;
----------------------
do $$
begin
for r in 1..365 loop
insert into public.cadence_details (frequency,entry_id,time_interval_in_days,created_time_stamp,created_by,modified_by) values('daily',r,'1',CURRENT_TIMESTAMP,'build','build');
end loop;
end;
$$;
----------------------
do $$
begin
for r in 1..24 loop
insert into public.cadence_details (frequency,entry_id,time_interval_in_days,created_time_stamp,created_by,modified_by) values('semimonthly',r,'15',CURRENT_TIMESTAMP,'build','build');
end loop;
end;
$$;
----------------------

CREATE OR REPLACE VIEW public.FAS_cadence_config AS
(   SELECT client_feed_config.client_id,
        client_feed_config.client_name,
        client_feed_config.feed_name,
        TRIM(BOTH '"'::text FROM (client_feed_config.feed_config -> 'feed_type'::text)::text) AS feed_type,
        TRIM(BOTH '"'::text FROM ((client_feed_config.feed_config -> 'frequency'::text) -> 'type'::text)::text) AS feed_frequency,
        split_part(TRIM(BOTH '"'::text FROM (((client_feed_config.feed_config -> 'extraction_settings'::text) -> 'implementation_setting'::text) -> 'periodic_start_date'::text)::text), 'T'::text, 1)::date AS periodic_start_date,
        TRIM(BOTH '"'::text FROM (((client_feed_config.feed_config -> 'extraction_settings'::text) -> 'implementation_setting'::text) -> 'lag_offset'::text)::text)::integer AS lag_offset,
        TRIM(BOTH '"'::text FROM (((client_feed_config.feed_config -> 'extraction_settings'::text) -> 'implementation_setting'::text) -> 'lag_tolerance'::text)::text)::integer AS lag_tolerance,
        TRIM(BOTH '"'::text FROM (((client_feed_config.feed_config -> 'extraction_settings'::text) -> 'implementation_setting'::text) -> 'notify_type'::text)::text) AS notify_type,
        split_part(TRIM(BOTH '"'::text FROM (((client_feed_config.feed_config -> 'extraction_settings'::text) -> 'implementation_setting'::text) -> 'adhoc_start_date'::text)::text), 'T'::text, 1)::date AS adhoc_start_date,
        split_part(TRIM(BOTH '"'::text FROM (((client_feed_config.feed_config -> 'extraction_settings'::text) -> 'implementation_setting'::text) -> 'adhoc_end_date'::text)::text), 'T'::text, 1)::date AS adhoc_end_date,
        TRIM(BOTH '"'::text FROM (((client_feed_config.feed_config -> 'extraction_settings'::text) -> 'implementation_setting'::text) -> 'is_adhoc_run'::text)::text)::boolean AS is_adhoc_run,
        TRIM(BOTH '"'::text FROM (((client_feed_config.feed_config -> 'extraction_settings'::text) -> 'implementation_setting'::text) -> 'split_by'::text)::text) AS adhoc_split_by,
        TRIM(BOTH '"'::text FROM (((client_feed_config.feed_config -> 'extraction_settings'::text) -> 'implementation_setting'::text) -> 'extraction_type'::text)::text) AS extraction_type,
        json_array_elements(client_feed_config.feed_config -> 'extraction_details'::text) AS file_details,
        'config' as source
        FROM client_feed_config
    UNION ALL 
        select cfc.client_id, cfc.client_name, cfc.feed_name, 
        TRIM(BOTH '"'::text FROM (cfc.feed_config -> 'feed_type'::text)::text) AS feed_type,
        pac.split_by as feed_frequency, 
        split_part(TRIM(BOTH '"'::text FROM (((cfc.feed_config -> 'extraction_settings'::text) -> 'implementation_setting'::text) -> 'periodic_start_date'::text)::text), 'T'::text, 1)::date AS periodic_start_date,
        TRIM(BOTH '"'::text FROM (((cfc.feed_config -> 'extraction_settings'::text) -> 'implementation_setting'::text) -> 'lag_offset'::text)::text)::integer AS lag_offset,
        TRIM(BOTH '"'::text FROM (((cfc.feed_config -> 'extraction_settings'::text) -> 'implementation_setting'::text) -> 'lag_tolerance'::text)::text)::integer AS lag_tolerance,
        TRIM(BOTH '"'::text FROM (((cfc.feed_config -> 'extraction_settings'::text) -> 'implementation_setting'::text) -> 'notify_type'::text)::text) AS notify_type,
        pac.adhoc_start_date, pac.adhoc_end_date, pac.is_adhoc_run, pac.split_by AS adhoc_split_by, pac.extraction_type, 
        json_array_elements(cfc.feed_config -> 'extraction_details'::text) AS file_details,
        'dynamic' as source 
        from (
            select cm.feed_name , fm.split_by as feed_frequency, null as periodic_start_date, 
            fm.adhoc_start_date, fm.adhoc_end_date, fm.is_adhoc_run, fm.split_by,  cm.extraction_type, cm.output_folder_name
            from file_master fm join cadence_master cm on fm.cadence_id = cm.cadence_id 
            where fm.is_adhoc_run = true and cm.extraction_type in ('sample', 'historic') and cm.feed_type = 'pull' and output_folder_name is not null
            group by cm.feed_name, fm.split_by, fm.adhoc_start_date, fm.adhoc_end_date, fm.is_adhoc_run, cm.extraction_type, cm.output_folder_name) as pac            
        join client_feed_config cfc on pac.feed_name = cfc.feed_name   
);
-----------------------
/*
This SQL snippet calculates the `window_start_date` and other derived fields for a data extraction process based on different feed frequencies (`daily`, `weekly`, `biweekly`, `monthly`, `semimonthly`, `yearly`). 
The logic varies depending on the frequency:
- For `daily`, the start date is incremented by a fixed number of days (`time_interval_in_days`) based on the `entry_id`.
- For `monthly`, the start date is incremented by a number of months derived from the `entry_id`.
- For `semimonthly`, the calculation is more complex:
  - The logic determines whether the `periodic_start_date` falls on or before the 15th of the month or after it.
  - The `entry_id` determines whether the date corresponds to the 1st half (1st to 15th) or the 2nd half (16th to end of the month) of the month:
    - **Odd `entry_id`**: Represents the 1st half of the month (1st to 15th).
    - **Even `entry_id`**: Represents the 2nd half of the month (16th to end of the month).
  - The logic alternates between these halves by using the modulo operation (`mod(c_1.entry_id, 2)`).
  - The calculation uses `date_trunc` to compute the start of the month and adds intervals (`15 days`, `1 mon`, etc.) to derive the correct date.
- For other frequencies, the logic defaults to a daily-like calculation.
The derived `window_start_date` is used to determine the time window for data extraction, and additional fields like `lag_offset` and `lag_tolerance` are included for further processing.
*/
CREATE OR REPLACE VIEW public.FAS_cadence_calculator AS
(   
    SELECT cfc.client_name,
        cfc.feed_name,
        cfc.feed_frequency,
        false AS is_adhoc_run,
        'periodic' AS extraction_type,
        cfc.notify_type,
        CASE
            WHEN c_1.frequency = 'daily'::text THEN cfc.periodic_start_date + (c_1.entry_id - 1) * c_1.time_interval_in_days
            WHEN c_1.frequency = 'monthly'::text THEN date(cfc.periodic_start_date + '1 mon'::interval * (c_1.entry_id - 1)::double precision)
            WHEN c_1.frequency = 'semimonthly'::text THEN
                date(CASE WHEN extract(day from cfc.periodic_start_date) <= 15 THEN
                    CASE WHEN mod(c_1.entry_id, 2) = 1 THEN
                        date_trunc('month'::text, cfc.periodic_start_date) + '1 mon'::interval * ((c_1.entry_id - 1) / 2)::double precision
                    ELSE
                        date_trunc('month'::text, cfc.periodic_start_date) + '1 mon'::interval * ((c_1.entry_id - 1) / 2)::double precision + '15 days'::interval
                    END
                ELSE
                    CASE WHEN mod(c_1.entry_id, 2) = 1 THEN
                        date_trunc('month'::text, cfc.periodic_start_date) + '1 mon'::interval * ((c_1.entry_id - 1) / 2)::double precision + '15 days'::interval
                    ELSE
                        date_trunc('month'::text, cfc.periodic_start_date) + '1 mon'::interval * (c_1.entry_id / 2)::double precision
                    END
                END)
            ELSE cfc.periodic_start_date + (c_1.entry_id - 1) * c_1.time_interval_in_days
        END AS window_start_date,
        cfc.lag_offset,
        cfc.lag_tolerance,
        CASE
            WHEN c_1.frequency = 'daily'::text THEN cfc.periodic_start_date + (c_1.entry_id - 1) * c_1.time_interval_in_days
            WHEN c_1.frequency = 'monthly'::text THEN date(cfc.periodic_start_date + '1 mon'::interval * c_1.entry_id::double precision + '1 day'::interval * (c_1.entry_id - (c_1.entry_id + 1))::double precision)
            WHEN c_1.frequency = 'semimonthly'::text THEN
                date(CASE WHEN extract(day from cfc.periodic_start_date) <= 15 THEN
                    CASE WHEN mod(c_1.entry_id, 2) = 1 THEN
                        date_trunc('month'::text, cfc.periodic_start_date) + '1 mon'::interval * ((c_1.entry_id - 1) / 2)::double precision + '14 days'::interval
                    ELSE
                        date_trunc('month'::text, cfc.periodic_start_date) + '1 mon'::interval * ((c_1.entry_id - 1) / 2)::double precision + '1 mon'::interval - '1 day'::interval
                    END
                ELSE
                    CASE WHEN mod(c_1.entry_id, 2) = 1 THEN
                        date_trunc('month'::text, cfc.periodic_start_date) + '1 mon'::interval * ((c_1.entry_id - 1) / 2)::double precision + '1 mon'::interval - '1 day'::interval
                    ELSE
                        date_trunc('month'::text, cfc.periodic_start_date) + '1 mon'::interval * (c_1.entry_id / 2)::double precision + '14 days'::interval
                    END
                END)
            ELSE cfc.periodic_start_date + c_1.entry_id * (c_1.time_interval_in_days - 1) + (c_1.entry_id - 1)
        END AS window_end_date,
        replace(TRIM(BOTH '"'::text FROM (cfc.file_details -> 'file_name_format'::text)::text), '\\'::text, '\'::text) AS file_name_format,
        TRIM(BOTH '"'::text FROM (cfc.file_details -> 'logical_file_name'::text)::text) AS logical_file_name,
        TRIM(BOTH '"'::text FROM (cfc.file_details -> 'is_mandatory'::text)::text)::boolean AS is_mandatory,
        generate_series(
                COALESCE(NULLIF(TRIM(BOTH '"'::text FROM (cfc.file_details -> 'part_start_seq'::text)::text), ''), '1')::integer,
                ((COALESCE(NULLIF(TRIM(BOTH '"'::text FROM (cfc.file_details -> 'part_start_seq'::text)::text), ''), '1')::integer -1) +
                COALESCE(NULLIF(TRIM(BOTH '"'::text FROM (cfc.file_details -> 'part_count'::text)::text), ''), '1')::integer)
            ) As part,
        NULL as output_folder,
        cfc.source
        FROM FAS_cadence_config cfc
            JOIN cadence_details c_1 ON cfc.feed_frequency = c_1.frequency AND cfc.is_adhoc_run IS FALSE
    UNION ALL
    SELECT a.client_name,
        a.feed_name,
        a.feed_frequency,
        a.is_adhoc_run,
        CASE WHEN
            a.is_adhoc_run = true THEN a.extraction_type
            ELSE null
        END AS extraction_type,
        a.notify_type,
        a.window_start_date,
        a.lag_offset,
        a.lag_tolerance,
        a.window_end_date,
        a.file_name_format,
        a.logical_file_name,
        a.is_mandatory,
        a.part,
        a.output_folder,
        a.source
        FROM ( 
            SELECT cfc.client_name,
            cfc.feed_name,
            cfc.adhoc_split_by AS feed_frequency,
            cfc.is_adhoc_run,
            cfc.extraction_type,
            cfc.notify_type,
            CASE WHEN cfc.feed_type = 'push' THEN 
                CASE
                    WHEN c_1.frequency = 'daily'::text THEN (cfc.adhoc_start_date - c_1.time_interval_in_days) + (c_1.entry_id) * c_1.time_interval_in_days
                    WHEN c_1.frequency = 'monthly'::text THEN date((cfc.adhoc_start_date - c_1.time_interval_in_days) + '1 mon'::interval * (c_1.entry_id)::double precision)
                    WHEN c_1.frequency = 'semimonthly'::text THEN
                        date(CASE WHEN extract(day from cfc.adhoc_start_date) <= 15 THEN
                            CASE WHEN mod(c_1.entry_id, 2) = 1 THEN
                                date_trunc('month'::text, cfc.adhoc_start_date) + '1 mon'::interval * ((c_1.entry_id - 1) / 2)::double precision
                            ELSE
                                date_trunc('month'::text, cfc.adhoc_start_date) + '1 mon'::interval * ((c_1.entry_id - 1) / 2)::double precision + '15 days'::interval
                            END
                        ELSE
                            CASE WHEN mod(c_1.entry_id, 2) = 1 THEN
                                date_trunc('month'::text, cfc.adhoc_start_date) + '1 mon'::interval * ((c_1.entry_id - 1) / 2)::double precision + '15 days'::interval
                            ELSE
                                date_trunc('month'::text, cfc.adhoc_start_date) + '1 mon'::interval * (c_1.entry_id / 2)::double precision
                            END
                        END)
                    ELSE (cfc.adhoc_start_date - c_1.time_interval_in_days) + (c_1.entry_id) * c_1.time_interval_in_days
                END
            ELSE 
                CASE
                    WHEN c_1.frequency = 'daily'::text THEN cfc.adhoc_start_date + (c_1.entry_id) * c_1.time_interval_in_days
                    WHEN c_1.frequency = 'monthly'::text THEN date(cfc.adhoc_start_date + '1 mon'::interval * (c_1.entry_id)::double precision)
                    WHEN c_1.frequency = 'semimonthly'::text THEN
                        date(CASE WHEN extract(day from cfc.adhoc_start_date) <= 15 THEN
                            CASE WHEN mod(c_1.entry_id, 2) = 1 THEN
                                date_trunc('month'::text, cfc.adhoc_start_date) + '1 mon'::interval * ((c_1.entry_id - 1) / 2)::double precision + '15 days'::interval
                            ELSE
                                date_trunc('month'::text, cfc.adhoc_start_date) + '1 mon'::interval * ((c_1.entry_id - 1) / 2)::double precision + '1 mon'::interval
                            END
                        ELSE
                            CASE WHEN mod(c_1.entry_id, 2) = 1 THEN
                                date_trunc('month'::text, cfc.adhoc_start_date) + '1 mon'::interval * ((c_1.entry_id - 1) / 2)::double precision + '1 mon'::interval
                            ELSE
                                date_trunc('month'::text, cfc.adhoc_start_date) + '1 mon'::interval * (c_1.entry_id / 2)::double precision + '15 days'::interval
                            END
                        END)
                    ELSE cfc.adhoc_start_date + (c_1.entry_id) * c_1.time_interval_in_days
                END 
            END AS window_start_date,
            cfc.lag_offset,
            cfc.lag_tolerance,
            CASE WHEN cfc.feed_type = 'push' THEN
                CASE
                    WHEN c_1.frequency = 'daily'::text THEN (cfc.adhoc_start_date - c_1.time_interval_in_days) + (c_1.entry_id ) * c_1.time_interval_in_days
                    WHEN c_1.frequency = 'monthly'::text THEN date((cfc.adhoc_start_date - c_1.time_interval_in_days) + '1 mon'::interval * (c_1.entry_id + 1)::double precision - '1 day'::interval)
                    WHEN c_1.frequency = 'semimonthly'::text THEN
                        date(CASE WHEN extract(day from cfc.adhoc_start_date) <= 15 THEN
                            CASE WHEN mod(c_1.entry_id, 2) = 1 THEN
                                date_trunc('month'::text, cfc.adhoc_start_date) + '1 mon'::interval * ((c_1.entry_id - 1) / 2)::double precision + '14 days'::interval
                            ELSE
                                date_trunc('month'::text, cfc.adhoc_start_date) + '1 mon'::interval * ((c_1.entry_id - 1) / 2)::double precision + '1 mon'::interval - '1 day'::interval
                            END
                        ELSE
                            CASE WHEN mod(c_1.entry_id, 2) = 1 THEN
                                date_trunc('month'::text, cfc.adhoc_start_date) + '1 mon'::interval * ((c_1.entry_id - 1) / 2)::double precision + '1 mon'::interval - '1 day'::interval
                            ELSE
                                date_trunc('month'::text, cfc.adhoc_start_date) + '1 mon'::interval * (c_1.entry_id / 2)::double precision + '14 days'::interval
                            END
                        END)
                    ELSE (cfc.adhoc_start_date - c_1.time_interval_in_days) + (c_1.entry_id + 1) * (c_1.time_interval_in_days - 1) + c_1.entry_id
                END
            ELSE 
                CASE
                    WHEN c_1.frequency = 'daily'::text THEN cfc.adhoc_start_date + (c_1.entry_id ) * c_1.time_interval_in_days
                    WHEN c_1.frequency = 'monthly'::text THEN date(cfc.adhoc_start_date + '1 mon'::interval * (c_1.entry_id + 1)::double precision - '1 day'::interval)
                    WHEN c_1.frequency = 'semimonthly'::text THEN
                        date(CASE WHEN extract(day from cfc.adhoc_start_date) <= 15 THEN
                            CASE WHEN mod(c_1.entry_id, 2) = 1 THEN
                                date_trunc('month'::text, cfc.adhoc_start_date) + '1 mon'::interval * ((c_1.entry_id - 1) / 2)::double precision + '1 mon'::interval - '1 day'::interval
                            ELSE
                                date_trunc('month'::text, cfc.adhoc_start_date) + '1 mon'::interval * (c_1.entry_id / 2)::double precision + '14 days'::interval
                            END
                        ELSE
                            CASE WHEN mod(c_1.entry_id, 2) = 1 THEN
                                date_trunc('month'::text, cfc.adhoc_start_date) + '1 mon'::interval * ((c_1.entry_id + 1) / 2)::double precision + '14 days'::interval
                            ELSE
                                date_trunc('month'::text, cfc.adhoc_start_date) + '1 mon'::interval * (c_1.entry_id / 2)::double precision + '1 mon'::interval - '1 day'::interval
                            END
                        END)
                    ELSE cfc.adhoc_start_date + (c_1.entry_id + 1) * (c_1.time_interval_in_days - 1) + c_1.entry_id
                END
            END AS window_end_date,
            replace(TRIM(BOTH '"'::text FROM (cfc.file_details -> 'file_name_format'::text)::text), '\\'::text, '\'::text) AS file_name_format,
            TRIM(BOTH '"'::text FROM (cfc.file_details -> 'logical_file_name'::text)::text) AS logical_file_name,
            TRIM(BOTH '"'::text FROM (cfc.file_details -> 'is_mandatory'::text)::text)::boolean AS is_mandatory,
            generate_series(
                COALESCE(NULLIF(TRIM(BOTH '"'::text FROM (cfc.file_details -> 'part_start_seq'::text)::text), ''), '1')::integer,
                ((COALESCE(NULLIF(TRIM(BOTH '"'::text FROM (cfc.file_details -> 'part_start_seq'::text)::text), ''), '1')::integer -1) +
                COALESCE(NULLIF(TRIM(BOTH '"'::text FROM (cfc.file_details -> 'part_count'::text)::text), ''), '1')::integer)
            ) As part,
            cfc.adhoc_end_date,
            CASE WHEN cfc.feed_type = 'push' THEN
                cfc.adhoc_end_date
            ELSE
                CASE
                    WHEN c_1.frequency = 'daily'::text THEN cfc.adhoc_end_date + c_1.time_interval_in_days
                    WHEN c_1.frequency = 'monthly'::text THEN date(cfc.adhoc_end_date + '1 mon'::interval)
                    ELSE cfc.adhoc_end_date + c_1.time_interval_in_days
                END
            END AS temp_end_date,
            CONCAT(TO_CHAR(cfc.adhoc_start_date,'YYYYMMDD'), '_' ,TO_CHAR(cfc.adhoc_end_date,'YYYYMMDD')) AS output_folder,
            cfc.source
            FROM FAS_cadence_config cfc
                JOIN cadence_details c_1 ON cfc.adhoc_split_by = c_1.frequency AND cfc.is_adhoc_run IS TRUE
        ) a
        WHERE a.window_start_date <= a.temp_end_date
    );


-----------------------

CREATE OR REPLACE VIEW public.FAS_detailed_window_calculator AS
    (   SELECT dfd.cadence_id,
        dfmd.feed_id,
        c.client_name,
        c.feed_name,
        c.feed_frequency,
        c.is_adhoc_run,
        c.notify_type,
        c.window_start_date,
        c.lag_offset,
            CASE
                WHEN c.is_adhoc_run IS TRUE THEN NULL::date
                WHEN c.feed_frequency = 'daily'::text THEN c.window_start_date
                ELSE c.window_start_date + c.lag_offset - 1
            END AS expectation_date,
        c.lag_tolerance,
            CASE
                WHEN c.is_adhoc_run IS TRUE THEN NULL::date
                WHEN c.feed_frequency = 'daily'::text THEN c.window_start_date
                ELSE c.window_start_date + c.lag_offset + c.lag_tolerance - 1
            END AS cutoff_date,
        c.window_end_date,
            CASE
                WHEN c.is_adhoc_run IS TRUE THEN NULL::date
                WHEN c.feed_frequency = 'daily'::text THEN NULL::date
                WHEN c.notify_type = 'once_post_window'::text THEN NULL::date
                WHEN c.notify_type = 'monthend'::text THEN NULL::date
                WHEN c.notify_type = 'once_per_cadence'::text AND c.feed_frequency IN ('monthly', 'yearly', 'quarterly', 'halfyearly') THEN ((c.window_start_date + c.lag_offset + c.lag_tolerance) - 1)
                ELSE c.window_start_date + c.lag_offset
            END AS yellow_email_start_date,
            CASE
                WHEN c.is_adhoc_run IS TRUE THEN NULL::date
                WHEN c.feed_frequency = 'daily'::text THEN NULL::date
                WHEN c.notify_type = 'once_post_window'::text THEN NULL::date
                WHEN c.notify_type = 'monthend'::text THEN NULL::date
                WHEN c.notify_type = 'once_per_cadence'::text AND c.feed_frequency IN ('monthly', 'yearly', 'quarterly', 'halfyearly') THEN ((c.window_start_date + c.lag_offset + c.lag_tolerance) - 1)
                ELSE c.window_start_date + c.lag_offset + c.lag_tolerance - 1
            END AS yellow_email_end_date,
            CASE
                WHEN c.is_adhoc_run IS TRUE THEN NULL::date
                WHEN c.feed_frequency = 'daily'::text THEN c.window_end_date + 2
                WHEN c.notify_type = 'once_post_window'::text THEN c.window_end_date + 1
                WHEN c.notify_type = 'monthend'::text THEN c.window_end_date + 2
                WHEN c.notify_type = 'once_per_cadence'::text AND c.feed_frequency IN ('monthly', 'yearly', 'quarterly', 'halfyearly') THEN ((c.window_start_date + c.lag_offset + (c.lag_tolerance * 2)) - 1)
                ELSE c.window_start_date + c.lag_offset + c.lag_tolerance
            END AS red_email_start_date,
            CASE
                WHEN c.is_adhoc_run IS TRUE THEN NULL::date
                WHEN c.feed_frequency = 'daily'::text THEN (date_trunc('month'::text, c.window_end_date::timestamp with time zone) + '1 mon'::interval - '1 day'::interval)::date + 2
                WHEN c.notify_type = 'once_post_window'::text THEN c.window_end_date + 1
                WHEN c.notify_type = 'monthend'::text THEN (date_trunc('month'::text, c.window_end_date::timestamp with time zone) + '1 mon'::interval - '1 day'::interval)::date + 2
                WHEN c.notify_type = 'once_per_cadence'::text AND c.feed_frequency IN ('monthly', 'yearly', 'quarterly', 'halfyearly') THEN ((c.window_start_date + c.lag_offset + (c.lag_tolerance * 2)) - 1)
                ELSE c.window_end_date
            END AS red_email_end_date,
        dfmd.file_name,
        c.file_name_format,
        c.logical_file_name,
        c.is_mandatory,
        c.extraction_type,
        c.part,
        c.output_folder,
        c.source
    FROM FAS_cadence_calculator c
        LEFT JOIN cadence_master dfd ON c.feed_name = dfd.feed_name AND dfd.feed_startdate >= c.window_start_date AND dfd.feed_startdate <= c.window_end_date AND c.extraction_type = dfd.extraction_type
        LEFT JOIN file_master dfmd ON dfd.cadence_id = dfmd.cadence_id AND lower(dfmd.file_name) ~ lower(c.file_name_format) AND c.extraction_type = dfmd.extraction_type AND c.part = dfmd.part
    );

-----------------------
CREATE OR REPLACE VIEW public.file_arrival_status AS 
(   SELECT * FROM FAS_detailed_window_calculator where source = 'config' );

-----------------------
CREATE OR REPLACE VIEW public.adhoc_file_arrival_status AS 
(   SELECT * FROM FAS_detailed_window_calculator where source = 'dynamic' );

--------------------------------
