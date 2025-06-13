<template>
  <div class="stars-container">
    <div
      v-for="star in stars"
      :key="star.id"
      class="star"
      :style="{
        left: `${star.x}%`,
        top: `${star.y}%`,
        width: `${star.size}px`,
        height: `${star.size}px`,
        opacity: star.opacity,
        transform: `translateY(${star.offset}px)`,
        animationDuration: `${star.duration}s`,
        animationDelay: `${star.delay}s`,
        backgroundColor: star.color,
      }"
    ></div>
  </div>
</template>

<script>
export default {
  name: "StarsBackground",
  data() {
    return {
      stars: [],
      numberOfStars: 50,
      starColors: ["#00b4ff", "#ffffff"],
    };
  },
  mounted() {
    this.generateStars();
  },
  methods: {
    generateStars() {
      this.stars = Array.from({ length: this.numberOfStars }, (_, index) => {
        const duration = Math.random() * 40 + 20; // Random duration between 30-45s
        return {
          id: index,
          x: Math.random() * 100,
          y: Math.random() * 100,
          size: Math.random() * 3.5 + 0.5,
          opacity: Math.random() * 0.3 + 0.7,
          delay: -Math.random() * duration, // Negative delay to start midway
          duration: duration,
          color:
            this.starColors[Math.floor(Math.random() * this.starColors.length)],
        };
      });
    },
  },
};
</script>

<style scoped>
.stars-container {
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100vh;
  background-color: #080e1a;
  overflow: hidden;
  z-index: -1;
}

.star {
  position: absolute;
  border-radius: 50%;
  animation: moveUpward linear infinite;
}

@keyframes moveUpward {
  from {
    transform: translateY(100vh);
  }
  to {
    transform: translateY(-100vh);
  }
}
</style>
