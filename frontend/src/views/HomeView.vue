<template>
  <SearchSection @search-results="handleSearchResults" />
  <GallerySection :searchResults="searchResults" />
  <BackToTop />
</template>

<script>
import BackToTop from "@/components/BackToTop.vue";
import SearchSection from "@/components/SearchSection.vue";
import GallerySection from "@/components/GallerySection.vue";

export default {
  name: "HomeView",
  components: {
    BackToTop,
    SearchSection,
    GallerySection,
  },
  data() {
    return {
      searchResults: [],
    };
  },
  methods: {
    handleSearchResults(results) {
      this.searchResults = results;
    },
  },
};
</script>
