<template>
  
  <div class="rounded bg-white p-2 shadow"> 
    <h4 class="font-semibold text-blue-600">{{title}}</h4>
    <slot></slot>
  </div>

</template>
<script>
export default {
  
  props: {
    title: {
      required: true
    }
  }

}
</script>