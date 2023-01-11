<template>
  <!--
  <img alt="Vue logo" src="./assets/logo.png">
  -->
  <sidebar-menu :menu="menu" />
  <preview-csv/>
</template>

<script>
import PreviewCsv from './components/PreviewCsv.vue';
import { SidebarMenu } from 'vue-sidebar-menu'
import 'vue-sidebar-menu/dist/vue-sidebar-menu.css'

export default {
  name: 'App',
  components: {
	PreviewCsv,
	SidebarMenu
  },
  data() {
    return {
      menu: [
        {
          header: 'Main Navigation',
          hiddenOnCollapse: true
        },
        {
          href: '/',
          title: 'Dashboard',
          icon: 'fa fa-user'
        },
        {
          href: '/charts',
          title: 'Charts',
          icon: 'fa fa-chart-area',
          child: [
            {
              href: '/charts/sublink',
              title: 'Sub Link'
            }
          ]
        }
      ]
    }
  },

}
</script>

@import "custom-var.scss";
@import "vue-sidebar-menu/src/scss/vue-sidebar-menu.scss";

<style>

#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
  margin-top: 60px;
	max-width: 600px;
	margin: auto;
}
</style>
