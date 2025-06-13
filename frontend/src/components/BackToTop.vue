<template>
  <div class="container">
    <button
      v-if="showBackToTopButton"
      class="action-button go-up-button"
      @click="scrollToTop"
    >
      <i class="fas fa-arrow-up"></i>
    </button>
  </div>
</template>

<script>
export default {
  name: 'BackToTop',
  components: {
  },
  data () {
    return {
      showBackToTopButton: false
    }
  },
  mounted () {
    window.addEventListener('scroll', this.handleScroll)
  },
  beforeUnmount () {
    window.removeEventListener('scroll', this.handleScroll)
  },
  methods: {
    scrollToTop () {
      window.scrollTo({
        top: 0,
        behavior: 'smooth'
      })
    },
    handleScroll () {
      this.showBackToTopButton = window.scrollY > 200
    }
  }
}
</script>

<style scoped>
.container {
  position: fixed;
  bottom: 1rem;
  right: 1rem;
  margin-right: 1rem;
}

.go-up-button {
  z-index: 1000;
  background: rgba(234, 94, 19, 0.8);
}

.go-up-button:hover {
  background: rgba(255, 111, 30, 0.9);
  transform: scale(1.1);
}
</style>
