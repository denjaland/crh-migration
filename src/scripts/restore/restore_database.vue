<template>
  <pre>
    -- Restore {{type}} database {{settings.databases[type].name}}
    USE [master]
    IF NOT EXISTS(SELECT * FROM sys.databases WHERE name = '{{settings.databases[type].name}}')
    BEGIN
      CREATE DATABASE [{{settings.databases[type].name}}]
    END
    GO
    ALTER DATABASE [{{settings.databases[type].name}}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
    RESTORE DATABASE [{{settings.databases[type].name}}] 
      FROM DISK = N'{{settings.databases[type].backup_file}}' 
      WITH FILE = 1, NOUNLOAD, REPLACE, STATS = 5,
      MOVE '{{settings.databases[type].files.data.logical}}' TO '{{settings.databases[type].files.data.physical}}',
      MOVE '{{settings.databases[type].files.log.logical}}' TO '{{settings.databases[type].files.log.physical}}'
    ALTER DATABASE [{{settings.databases[type].name}}] SET MULTI_USER
    GO
  </pre>
</template>
<script>
import settings from '../../settings';
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