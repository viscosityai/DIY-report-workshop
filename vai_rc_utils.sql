-- Create a custom record type to hold different data types
CREATE OR REPLACE TYPE t_varchar_array_record AS OBJECT (
    nm_val NUMBER,
    v2_val VARCHAR2(4000),
    cb_val CLOB,
    dt_val DATE
);
/

-- Create a nested table type based on the custom record type
CREATE OR REPLACE TYPE t_varchar_array IS TABLE OF t_varchar_array_record;
/

CREATE OR REPLACE PACKAGE VAI_RC_UTILS AS
/*
    @Purpose: Provides utilities for generating and storing AOP reports
    @Author: Diego Fion
    @Date: 2023-04-27
*/

/*
    @Function: get_json_object_from_raw_query
    @Purpose: Converts a given raw SQL query into a JSON query notation
    @param p_sql: The raw SQL query to be converted
    @param p_datasources_common_parameters: An optional array of dynamic parameters to be used in the SQL query
    @return: A CLOB containing the converted SQL query in JSON notation
    @Note: 
        t_varchar_array_record supported types are:
            nm_val -> NUMBER
            v2_val -> VARCHAR2
            cb_val -> CLOB
            dt_val -> DATE
    @Example: 
        DECLARE
            l_sql CLOB := 'SELECT * FROM employees WHERE department_id = #P1# AND manager_id = #P2# AND job_id = #P3#';
            l_params t_varchar_array := t_varchar_array(); -- Initialize the nested table
        BEGIN
            l_params.extend(3);
            l_params(1) := t_varchar_array_record(10, NULL, empty_clob(), NULL); -- Number (department_id)
            l_params(2) := t_varchar_array_record(101, NULL, empty_clob(), NULL); -- Number (manager_id)
            l_params(3) := t_varchar_array_record(NULL, 'IT_PROG', empty_clob(), NULL); -- Varchar2 (job_id)

            DBMS_OUTPUT.PUT_LINE(VAI_RC_UTILS.get_json_object_from_raw_query(l_sql, l_params));
        END;
*/
    FUNCTION get_json_object_from_raw_query(p_sql IN CLOB
                                            , p_datasources_common_parameters IN t_varchar_array DEFAULT NULL) RETURN CLOB;

/*
    @Function: get_json_object
    @Purpose: Generates a JSON object based on data source IDs
    @param p_datasource_ids: A CLOB containing a comma-separated list of data source IDs
    @param p_datasources_common_parameters: An optional array of dynamic parameters to be used in the SQL query
    @return: A CLOB containing the JSON object
    @Note: 
        t_varchar_array_record supported types are:
            nm_val -> NUMBER
            v2_val -> VARCHAR2
            cb_val -> CLOB
            dt_val -> DATE
    @Example:
        DECLARE
            l_datasource_ids CLOB := '1,2,3';
            l_params t_varchar_array := t_varchar_array(); -- Initialize the nested table
        BEGIN
            l_params.extend(3);
            l_params(1) := t_varchar_array_record(10, NULL, empty_clob(), NULL);
            l_params(2) := t_varchar_array_record(101, NULL, empty_clob(), NULL);
            l_params(3) := t_varchar_array_record(NULL, 'IT_PROG', empty_clob(), NULL);

            DBMS_OUTPUT.PUT_LINE(VAI_RC_UTILS.get_json_object(l_datasource_ids, l_params));
        END;
*/
    FUNCTION get_json_object(p_datasource_ids CLOB
                             , p_datasources_common_parameters IN t_varchar_array DEFAULT NULL) RETURN CLOB;

/*
    @Procedure: store_aop_report
    @Purpose: Generates and stores an AOP report in the database
    @param p_report_name: The name of the report to be generated and stored
    @param p_datasources_common_parameters: An optional array of dynamic parameters to be used in the SQL query
    @Note: 
        t_varchar_array_record supported types are:
            nm_val -> NUMBER
            v2_val -> VARCHAR2
            cb_val -> CLOB
            dt_val -> DATE
    @Example:
        DECLARE
            l_datasource_ids CLOB := '1,2,3';
            l_params t_varchar_array := t_varchar_array(); -- Initialize the nested table
        BEGIN
            l_params.extend(3);
            l_params(1) := t_varchar_array_record(10, NULL, empty_clob(), NULL);
            l_params(2) := t_varchar_array_record(101, NULL, empty_clob(), NULL);
            l_params(3) := t_varchar_array_record(NULL, 'IT_PROG', empty_clob(), NULL);
            
            VAI_RC_UTILS.store_aop_report(l_report_name, l_params);
        END;
*/
    PROCEDURE store_aop_report(p_report_name VARCHAR2
                               , p_datasources_common_parameters IN t_varchar_array DEFAULT NULL);

END VAI_RC_UTILS;
/

create or replace PACKAGE BODY VAI_RC_UTILS AS
    c_aop_api_url VARCHAR2(500) := 'https://aop.viscosity.ai/';
    c_main_app_id NUMBER := 1010;

/*
    @Purpose: Provides utilities for generating and storing AOP reports
    @Author: Diego Fion
    @Date: 2023-04-27
*/

/*
    @Function: get_json_object_from_raw_query
    @Purpose: Converts a given raw SQL query into a JSON query notation
    @param p_sql: The raw SQL query to be converted
    @param p_datasources_common_parameters: An optional array of dynamic parameters to be used in the SQL query
    @return: A CLOB containing the converted SQL query in JSON notation
    @Note: 
        t_varchar_array_record supported types are:
            nm_val -> NUMBER
            v2_val -> VARCHAR2
            cb_val -> CLOB
            dt_val -> DATE
    @Example: 
        DECLARE
            l_sql CLOB := 'SELECT * FROM employees WHERE department_id = #P1# AND manager_id = #P2# AND job_id = #P3#';
            l_params t_varchar_array := t_varchar_array(); -- Initialize the nested table
        BEGIN
            l_params.extend(3);
            l_params(1) := t_varchar_array_record(10, NULL, empty_clob(), NULL); -- Number (department_id)
            l_params(2) := t_varchar_array_record(101, NULL, empty_clob(), NULL); -- Number (manager_id)
            l_params(3) := t_varchar_array_record(NULL, 'IT_PROG', empty_clob(), NULL); -- Varchar2 (job_id)

            DBMS_OUTPUT.PUT_LINE(VAI_RC_UTILS.get_json_object_from_raw_query(l_sql, l_params));
        END;
*/
    FUNCTION get_json_object_from_raw_query(p_sql IN CLOB
                                            , p_datasources_common_parameters IN t_varchar_array DEFAULT NULL) RETURN CLOB
    IS
        l_json_query CLOB;
        l_cursor      INTEGER;
        l_col_cnt     INTEGER;
        l_desc_tab    DBMS_SQL.DESC_TAB;
        l_raw_query   CLOB;
        l_r_query     CLOB;
        l_param_str   CLOB;
    BEGIN
        l_r_query := p_sql;
        
        DBMS_OUTPUT.PUT_LINE('DEBUG>> get_json_object_from_raw_query >> verifying if p_sql variable has parameters and if the datasource parameters are not empty');
        
        /*IF REGEXP_INSTR(l_r_query, '#P\d+#') > 0 AND p_datasources_common_parameters IS NOT NULL THEN
            FOR i IN 1..p_datasources_common_parameters.COUNT LOOP
                DBMS_OUTPUT.PUT_LINE('DEBUG>> get_json_object_from_raw_query >> replacing parameter #P'||i||'# with '|| p_datasources_common_parameters(i));
                l_r_query := REPLACE(l_r_query, '#P' || i || '#', p_datasources_common_parameters(i));
            END LOOP;
        END IF;*/

        IF REGEXP_INSTR(l_r_query, '#P\d+#') > 0 AND p_datasources_common_parameters IS NOT NULL THEN
            FOR i IN 1..p_datasources_common_parameters.COUNT LOOP
                l_param_str := CASE
                                WHEN p_datasources_common_parameters(i).nm_val IS NOT NULL THEN TO_CHAR(p_datasources_common_parameters(i).nm_val)
                                WHEN p_datasources_common_parameters(i).v2_val IS NOT NULL THEN '''' || p_datasources_common_parameters(i).v2_val || ''''
                                WHEN p_datasources_common_parameters(i).cb_val IS NOT NULL THEN '''' || p_datasources_common_parameters(i).cb_val || ''''
                                WHEN p_datasources_common_parameters(i).dt_val IS NOT NULL THEN 'TO_DATE(''' || TO_CHAR(p_datasources_common_parameters(i).dt_val, 'YYYY-MM-DD HH24:MI:SS') || ''', ''YYYY-MM-DD HH24:MI:SS'')'
                              END;

                l_r_query := REPLACE(l_r_query, '#P' || i || '#', l_param_str);
            END LOOP;
        END IF;

        l_raw_query := l_r_query;

        DBMS_OUTPUT.PUT_LINE('DEBUG>> get_json_object_from_raw_query >> l_raw_query query after applying REPLACE operation: '||l_raw_query);
        -- Parse the input query using DBMS_SQL
        l_cursor := DBMS_SQL.OPEN_CURSOR;

        DBMS_SQL.PARSE(l_cursor, l_raw_query, DBMS_SQL.NATIVE);
        DBMS_SQL.DESCRIBE_COLUMNS(l_cursor, l_col_cnt, l_desc_tab);
        DBMS_SQL.CLOSE_CURSOR(l_cursor);

        -- Initialize the JSON query with JSON_OBJECT functions
        l_json_query := 'SELECT JSON_ARRAYAGG(JSON_OBJECT(';

        -- Loop through the columns and append them to the JSON query
        FOR i IN 1 .. l_col_cnt LOOP
            l_json_query := l_json_query || 'KEY ' || '''' || NVL(l_desc_tab(i).col_name, 'COL_' || i) || ''' VALUE ' || 't.' || l_desc_tab(i).col_name || ', ';
        END LOOP;

        -- Remove the trailing comma and space
        l_json_query := RTRIM(l_json_query, ', ');

        -- Close the JSON_OBJECT function and append the original input query
        l_json_query := l_json_query || ') RETURNING CLOB) FROM (' || l_raw_query || ') t';

        RETURN l_json_query;
    END get_json_object_from_raw_query;


/*
    @Function: get_json_object
    @Purpose: Generates a JSON object based on data source IDs
    @param p_datasource_ids: A CLOB containing a comma-separated list of data source IDs
    @param p_datasources_common_parameters: An optional array of dynamic parameters to be used in the SQL query
    @return: A CLOB containing the JSON object
    @Note: 
        t_varchar_array_record supported types are:
            nm_val -> NUMBER
            v2_val -> VARCHAR2
            cb_val -> CLOB
            dt_val -> DATE
    @Example:
        DECLARE
            l_datasource_ids CLOB := '1,2,3';
            l_params t_varchar_array := t_varchar_array(); -- Initialize the nested table
        BEGIN
            l_params.extend(3);
            l_params(1) := t_varchar_array_record(10, NULL, empty_clob(), NULL);
            l_params(2) := t_varchar_array_record(101, NULL, empty_clob(), NULL);
            l_params(3) := t_varchar_array_record(NULL, 'IT_PROG', empty_clob(), NULL);

            DBMS_OUTPUT.PUT_LINE(VAI_RC_UTILS.get_json_object(l_datasource_ids, l_params));
        END;
*/
    FUNCTION get_json_object(p_datasource_ids CLOB
                             , p_datasources_common_parameters IN t_varchar_array DEFAULT NULL) RETURN CLOB
    IS
        l_dynamic_sql CLOB;
        l_query_open CLOB;
        l_query_close CLOB;
        l_datasourcequery CLOB;
        l_datasources CLOB;
        l_clob CLOB;
        l_count NUMBER := 0;
        l_dynamic_cur SYS_REFCURSOR;
        l_row VAI_RC_DATASOURCES%ROWTYPE;
    BEGIN
        -- Define Dynamic Query
        l_dynamic_sql := 'SELECT * FROM VAI_RC_DATASOURCES WHERE 1=1 AND id IN ('||p_datasource_ids||')';
        l_query_open := q'[SELECT JSON_ARRAYAGG( JSON_OBJECT( 'filename' value 'file1', 'data' value (SELECT JSON_ARRAYAGG( JSON_OBJECT( ]';
        l_query_close := q'[ ) RETURNING CLOB ) FROM DUAL) RETURNING CLOB )RETURNING CLOB ) AS aop_json FROM dual ]';

        -- Open a cursor for the dynamic SQL query
        OPEN l_dynamic_cur FOR l_dynamic_sql;

        LOOP
            FETCH l_dynamic_cur INTO l_row;
            EXIT WHEN l_dynamic_cur%NOTFOUND;
            
            DBMS_OUTPUT.PUT_LINE('DEBUG>> get_json_object >> calling get_json_object_from_raw_query using '||l_row.datasource_key||' datasource ');
            IF l_count > 0 THEN
                l_datasourcequery := q'[, ']'||LOWER(l_row.datasource_key)||q'[' value (]'||get_json_object_from_raw_query(l_row.raw_query,p_datasources_common_parameters)||q'[)]';
            ELSE
                l_datasourcequery := q'[ ']'||LOWER(l_row.datasource_key)||q'[' value (]'||get_json_object_from_raw_query(l_row.raw_query,p_datasources_common_parameters)||q'[)]';
            END IF;

            l_datasources := l_datasources||l_datasourcequery;
            l_count := l_count+1;
        END LOOP;

        l_datasources := l_datasources||q'[ RETURNING CLOB]';
        l_query_open := l_query_open||l_datasources;

        -- Close the cursor
        CLOSE l_dynamic_cur;

        DBMS_OUTPUT.PUT_LINE('DEBUG>> get_json_object >> l_query_open||l_query_close => '||l_query_open||l_query_close);

        RETURN l_query_open||l_query_close;
    END get_json_object;

/*
    @Procedure: store_aop_report
    @Purpose: Generates and stores an AOP report in the database
    @param p_report_name: The name of the report to be generated and stored
    @param p_datasources_common_parameters: An optional array of dynamic parameters to be used in the SQL query
    @Note: 
        t_varchar_array_record supported types are:
            nm_val -> NUMBER
            v2_val -> VARCHAR2
            cb_val -> CLOB
            dt_val -> DATE
    @Example:
        DECLARE
            l_datasource_ids CLOB := '1,2,3';
            l_params t_varchar_array := t_varchar_array(); -- Initialize the nested table
        BEGIN
            l_params.extend(3);
            l_params(1) := t_varchar_array_record(10, NULL, empty_clob(), NULL);
            l_params(2) := t_varchar_array_record(101, NULL, empty_clob(), NULL);
            l_params(3) := t_varchar_array_record(NULL, 'IT_PROG', empty_clob(), NULL);

            VAI_RC_UTILS.store_aop_report(l_report_name, l_params);
        END;
*/
    PROCEDURE store_aop_report(p_report_name VARCHAR2
                               , p_datasources_common_parameters IN t_varchar_array DEFAULT NULL)
    IS
        l_id            NUMBER;
        l_file_output   BLOB := empty_blob();
        l_template      BLOB := empty_blob();
        l_mime_type     VARCHAR2(300) := 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
        l_filename      VARCHAR2(4000) := 'json_poc';
        l_json          CLOB;
        l_aop_source_template   CLOB;
        l_report_template_id VARCHAR2(100);
        l_report_datasources_ids VARCHAR2(1000);
        l_output_type VARCHAR2(50);
    BEGIN
        --aop_api_pkg.g_debug := 'Local';
        SELECT
            TRIM(p_report_name)||'-'||TO_CHAR(systimestamp,'MMDDYYY_HH24MISS')||'.xlsx'
            INTO l_filename
        FROM DUAL;

        INSERT INTO vai_rc_report_downloads
        (name, status, body, mime_type)
        VALUES
        (l_filename,'PENDING',l_file_output,l_mime_type)
        RETURNING id into l_id;      

        -- @TODO find datasources based on report name
        -- Set Template ID and Datasources IDs
        SELECT
            TO_CHAR(rc_template_id)
            , listagg(rc_datasource_id,',') within group (order by rc_datasource_id) as datasources
            INTO 
                l_report_template_id
                , l_report_datasources_ids
        from vai_rc_reports_meta
        where 1=1
        and upper(name) = upper(p_report_name)
        group by TO_CHAR(rc_template_id);

        -- Get json
        SELECT get_json_object(p_datasource_ids => l_report_datasources_ids, p_datasources_common_parameters => p_datasources_common_parameters) 
            INTO l_json
        FROM DUAL;

        -- @TODO find template based on Report Name        
        l_aop_source_template := q'[SELECT template_type as template_type, 
                                            content as template_blob
                                    FROM vai_rc_templates
                                    WHERE 1=1
                                        AND id = ]'||l_report_template_id;
        SELECT
            template_type
            INTO l_output_type
        FROM vai_rc_templates
        WHERE 1=1
        AND id = l_report_template_id;

        l_file_output := aop_api_pkg.plsql_call_to_aop( p_data_type       =>  'JSON',
                                                        p_data_source     =>  l_json,
                                                        p_template_type   =>  'SQL',
                                                        p_template_source =>  l_aop_source_template,
                                                        p_output_type     =>  l_output_type,
                                                        p_output_filename =>  l_filename,
                                                        p_app_id          =>  c_main_app_id,
                                                        p_aop_url         =>  c_aop_api_url,
                                                        p_api_key         =>  NULL,
                                                        p_aop_mode        =>  ''
                                                    --p_aop_remote_debug => 'Local'
                                                );                                                    

        UPDATE vai_rc_report_downloads
            SET status = 'READY',
                body = l_file_output
        WHERE id = l_id;

    END store_aop_report;

END VAI_RC_UTILS;
