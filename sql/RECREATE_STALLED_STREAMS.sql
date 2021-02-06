create or replace procedure RECREATE_STALLED_STREAMS(DB_NAME Varchar)
/*
 *  Procedure to recreate all stalled streams of the database "DB_NAME".
 *  Returns the list of streams recreated, with the error messages if any.
 *
 *  V1.0 Lilian Arnaud
 *
 * Example of usage (running every Sunday at 6am UTC):
 * ---------------------------------------------------
 * 
 * create or replace TASK TASK_HOUSEKEEPING_STALLED_STREAMS
 *    WAREHOUSE = <Warehouse>
 *   SCHEDULE = 'USING CRON 0 6 * * SUN UTC'
 * AS
 * call COMMON_SCHEMA.RECREATE_STALLED_STREAMS(CURRENT_DATABASE());;
 *
 * grant ownership on task TASK_HOUSEKEEPING_STALLED_STREAMS to <DB_SYSADMIN_ROLE> REVOKE CURRENT GRANTS;
 * alter task TASK_HOUSEKEEPING_STALLED_STREAMS resume;
 */
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
if (DB_NAME == '' || DB_NAME == null) {
    return 'Error - Missing Parameter: Database Name';
}

var cmd = "show streams in database " + DB_NAME +";"
var stmt = snowflake.createStatement(
     {
       sqlText: cmd
     }
   );
   var r="recreated streams: ";
   var resultSet = stmt.execute (stmt);
   var sql = "select sysdate();"
   var sql = `select "database_name"||'.'||"schema_name"||'.'|| "name" stream_name, 'create or replace stream ' || "database_name"||'.'||"schema_name"||'.'|| "name" || ' on TABLE ' || "table_name" || case when "mode" = 'APPEND_ONLY' then ' APPEND_ONLY = TRUE;' else ';' end  from table(result_scan(last_query_id()))  where "stale" = true;`;

   stmt = snowflake.createStatement(
     {
       sqlText: sql
     }
   );

       
   var resultSet = stmt.execute (stmt);
   while (resultSet.next())  {
      var stream_name = resultSet.getColumnValue(1) ;
      var stream_sql = resultSet.getColumnValue(2) ;
      r += stream_name;
      try {
        snowflake.execute({
                sqlText: stream_sql
               }); 
      } catch (err)  {
        r+=": ["+err+"]";
      }
      r+="; "
   }
   return r;

$$;
