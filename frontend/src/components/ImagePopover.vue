<template>
  <Transition name="fade">
    <div v-if="show" class="popover-overlay" @click="$emit('close')">
      <Transition name="scale">
        <div class="popover-content" v-if="show">
          <div class="button-container">
            <button
              class="action-button close-button"
              @click.stop="$emit('close')"
              v-tooltip.top="'Close'"
            >
              <i class="fas fa-times"></i>
            </button>
          </div>
          <img
            :src="imageUrl"
            :alt="imageTitle"
            class="popover-image"
            @click.stop
          />
        </div>
      </Transition>
    </div>
  </Transition>
</template>

<script>
export default {
  name: "ImagePopover",
  props: {
    show: Boolean,
    imageUrl: String,
    imageTitle: String,
  },
  mounted() {
    document.addEventListener("keydown", this.handleEscape);
  },
  beforeUnmount() {
    document.removeEventListener("keydown", this.handleEscape);
  },
  methods: {
    handleEscape(e) {
      if (e.key === "Escape" && this.show) {
        this.$emit("close");
      }
    },
  },
};
</script>

<style scoped>
.popover-overlay {
  position: fixed;
  top: 0;
  left: 0;
  width: 100%;
  height: 100%;
  background: rgba(0, 0, 0, 0.9);
  display: flex;
  justify-content: center;
  align-items: center;
  z-index: 1000;
}

.popover-content {
  position: relative;
  max-width: 90vw;
  max-height: 100vh;
  padding-top: 3rem;
}

.popover-image {
  max-width: 100%;
  max-height: 90vh;
  object-fit: contain;
  border: 2px solid var(--primary-color);
  box-shadow: 0 0 1.25rem rgba(234, 94, 19, 0.3);
}

.button-container {
  position: absolute;
  top: 0rem;
  right: 0rem;
  display: flex;
  z-index: 1;
}

.close-button {
  background: rgba(234, 94, 19, 0.8);
}

.close-button:hover {
  background: rgba(255, 111, 30, 0.9);
  transform: scale(1.1);
}

/* Fade transition */
.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.3s ease;
}

.fade-enter-from,
.fade-leave-to {
  opacity: 0;
}

/* Scale transition */
.scale-enter-active,
.scale-leave-active {
  transition: transform 0.3s ease;
}

.scale-enter-from,
.scale-leave-to {
  transform: scale(0.9);
}
</style>
