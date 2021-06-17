<template>
  <div class="p-4 m-4 bg-gray-50 rounded-lg shadow">
    <h2 class="font-semibold text-2xl">Migration CRH</h2>
    <h3 class="text-xl mb-2">Generate script options</h3>
    <div class="grid grid-cols-3 gap-2">
  
      <option-container title="Restore databases">
        <option-check name="Source database" v-model="restore.source"></option-check>
        <option-check name="Target database" v-model="restore.target"></option-check>
      </option-container>
      
    </div>
    
  </div>

  <div class="p-4 m-4 bg-blue-100 rounded-lg shadow">
    <h2 class="font-semibold text-2xl">Result</h2>
    <pre><code>
      <restore-db type="source" v-if="restore.source"></restore-db>
      <restore-db type="target" v-if="restore.target"></restore-db>
    </code></pre>
  </div>
  

</template>

<script>
import {ref} from 'vue'
import OptionCheck from './components/option-check';
import OptionContainer from './components/option-container';
import RestoreDb from './scripts/restore/restore_database';
export default {
  name: "App",
  components: {
    OptionCheck,
    OptionContainer,
    RestoreDb
  },
  setup() {

    let restore = ref({source: false, target: true});

    return {
      restore
    };
  },
};
</script>


