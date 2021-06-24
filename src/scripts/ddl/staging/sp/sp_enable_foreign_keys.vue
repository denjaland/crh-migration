<template>
  <pre>
    USE [{{settings.databases[type].name}}]
    GO

    SET ANSI_NULLS ON
    GO

    SET QUOTED_IDENTIFIER ON
    GO

    IF object_id('[migration].[enableAllForeignKeys]') IS NULL
      EXEC ('create procedure dbo.[migration].[enableAllForeignKeys] as select 1')
    GO

    ALTER PROCEDURE dbo.[migration].[enableAllForeignKeys]
    AS
    BEGIN
      SET NOCOUNT ON;

      DECLARE @sql NVARCHAR(MAX) = N'';

      ;WITH x AS
          (
            SELECT DISTINCT obj =
                                QUOTENAME(OBJECT_SCHEMA_NAME(parent_object_id)) + '.'
                                + QUOTENAME(OBJECT_NAME(parent_object_id))
            FROM sys.foreign_keys
          )
      SELECT @sql += N'ALTER TABLE ' + obj + ' WITH CHECK CHECK CONSTRAINT ALL;
'     FROM x;

      EXEC sp_executesql @sql;
    END
    GO
  </pre>
</template>
<script>
import settings from '../../../settings';
export default {
  props: {
    type: {
      required: true
    }
  },
  setup() {
    return {
      settings
    }
  }
  
}
</script>