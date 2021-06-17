<template>
  <pre><code>
    -- Restore {{type}} database {{settings.databases[type].name}}
    USE [master]
    ALTER DATABASE [{{settings.databases[type].name}}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
    RESTORE DATABASE [{{settings.databases[type].name}}] FROM  DISK = N'{{settings.databases[type].backup_file}}' WITH  FILE = 1,  NOUNLOAD,  REPLACE,  STATS = 5
    ALTER DATABASE [{{settings.databases[type].name}}] SET MULTI_USER
    GO

  </code></pre>
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