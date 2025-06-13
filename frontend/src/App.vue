<template>
  <StarsBackground />
  <router-view />
</template>

<script>
import StarsBackground from "@/components/StarsBackground.vue";

export default {
  name: "App",
  components: {
    StarsBackground,
  },
};
</script>

<style>
/* Customize PrimeVue's default color */
:root {
  --primary-color: #ea5e13;
  --p-primary-color: var(--primary-color) !important;
  --p-select-placeholder-color: #fff !important;
  --p-select-dropdown-color: #fff !important;
  --p-select-clear-icon-color: #fff !important;
  --p-select-lg-font-size: 1.75rem !important;
}

body {
  font-family: "Roboto Condensed", sans-serif;
}

.container {
  display: flex;
  flex-direction: column;
  align-items: center;
  margin-left: 20rem;
  margin-right: 20rem;
}

.action-button {
  width: 2.5rem;
  height: 2.5rem;
  border: none;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  transition: all 0.3s ease;
  backdrop-filter: blur(5px);
}

.action-button i {
  font-size: 1rem;
  color: white;
}

.medium-text {
  font-size: 1.75rem !important;
}

.small-text {
  font-size: 1.5rem !important;
}

.align-vertical-center {
  display: flex;
  align-items: center;
}

.align-space-between {
  display: flex;
  justify-content: space-between;
}

.align-horizontal {
  display: flex;
}

@media screen and (max-width: 1920px) {
  :root {
    --p-select-lg-font-size: 1.2rem !important;
  }

  .container {
    margin-left: 12rem;
    margin-right: 12rem;
  }

  .medium-text {
    font-size: 1.2rem !important;
  }

  .small-text {
    font-size: 1rem !important;
  }
}

@media screen and (max-width: 1440px) {
  .container {
    margin-left: 5rem;
    margin-right: 5rem;
  }
}

@media screen and (max-width: 992px) {
  .container {
    margin-left: 3rem;
    margin-right: 3rem;
  }
}

@media screen and (max-width: 768px) {
  :root {
    --p-select-lg-font-size: 0.8rem !important;
  }
  .container {
    margin-left: 3rem;
    margin-right: 3rem;
  }

  .medium-text {
    font-size: 0.8rem !important;
  }

  .small-text {
    font-size: 0.6rem !important;
  }
}

@media screen and (max-width: 576px) {
  .container {
    margin-left: 2rem;
    margin-right: 2rem;
  }
}

@media screen and (max-width: 480px) {
  .container {
    margin-left: 1rem;
    margin-right: 1rem;
  }
}

</style>
