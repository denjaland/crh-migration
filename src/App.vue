<template>
  <div class="p-4 m-4 bg-gray-50 rounded-lg shadow">
    <h2 class="font-semibold text-2xl">Migration CRH</h2>
    <h3 class="text-xl mb-2">Generate script options</h3>
    <div class="grid grid-cols-3 gap-2">
  
      <option-container title="Restore databases">
        <option-check name="Source database" v-model="restore.source"></option-check>
        <option-check name="Target database" v-model="restore.target"></option-check>
        <option-check name="Staging database" v-model="restore.staging"></option-check>
      </option-container>

      <option-container title="DDL Preparations">
        <option-check name="Source database" v-model="ddl.source"></option-check>
        <option-check name="Target database" v-model="ddl.target"></option-check>
        <option-check name="Staging database" v-model="ddl.staging"></option-check>
      </option-container>
      
    </div>
    
  </div>

  <div class="p-4 m-4 bg-blue-100 rounded-lg shadow">
    <h2 class="font-semibold text-2xl">Result</h2>
    <pre style="user-select:contain"><code>
      <restore-db type="source" v-if="restore.source"></restore-db>
      <restore-db type="target" v-if="restore.target"></restore-db>
      <restore-db type="staging" v-if="restore.staging"></restore-db>

      <ddl-db type="source" v-if="ddl.source"></ddl-db>
      <ddl-db type="target" v-if="ddl.target"></ddl-db>
      <ddl-db type="staging" v-if="ddl.staging"></ddl-db>
    </code></pre>
  </div>
  

</template>

<script>
import {ref} from 'vue'
import OptionCheck from './components/option-check';
import OptionContainer from './components/option-container';
import RestoreDb from './scripts/restore/restore_database';
import DdlDb from './scripts/ddl/index';
export default {
  name: "App",
  components: {
    OptionCheck,
    OptionContainer,
    RestoreDb,
    DdlDb
  },
  setup() {

    let restore = ref({source: false, target: true, staging: true});
    let ddl = ref({source: false, target: true, staging: true});

    return {
      restore,
      ddl
    };
  },
};
</script>


