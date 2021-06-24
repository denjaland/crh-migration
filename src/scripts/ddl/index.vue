<template>
  
</template>

<script>
  import DdlSource from './source/index';
  import DdlTarget from './source/targe';
  import DdlStaging from './source/staging';

  export default {
    components: {
      DdlSource, 
      DdlTarget, 
      DdlStaging
    },
    props: {
      type: {
        required: true
      }      
    }
  }
</script>