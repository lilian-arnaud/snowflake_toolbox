# snowflake toolbox
stored procedures and tools around SnowFlake.

for databases with low activity, streams stall after 14 days, and the only way to fix this problem is to recreate stalled streams.
the procedure ```RECREATE_STALL_STREAMS```

after create, just execute the stored procedure with the name of the database in parameter:
```sql
call RECREATE_STALLED_STREAMS(CURRENT_DATABASE());
```
