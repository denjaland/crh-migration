<template>
  
  <div class="flex flex-row items-center">
    <input type="checkbox" value="true" :checked="modelValue" @change="updateInput"/> 
    <div class="ml-2">{{name}}</div>
  </div>
  
</template>
<script>
export default {
  
  props: {
    modelValue: {
      required: true
    },
    name: {
      required: true
    }
  },
  setup(props, {emit}) {
    let updateInput= function(event) {
      emit('update:modelValue', event.target.checked);
    }

    return {
      updateInput
    }
  }

}
</script>