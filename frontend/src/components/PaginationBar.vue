<template>
  <div class="pagination-bar-container">
    <div class="row">
      <div class="year-filter">
        <span class="medium-text">Year</span>
        <Select
          checkmark
          showClear=""
          class="custom-selector align-vertical-center"
          v-model="selectedYear"
          :options="yearOptions"
          placeholder="All years"
          :highlightOnSelect="false"
          @update:modelValue="handleYearChange"
          size="large"
          v-tooltip.bottom="'Filter by year'"
        />
      </div>

      <div class="search-method">
        <span class="medium-text">Search method</span>
        <Select
          checkmark
          class="custom-selector align-vertical-center"
          v-model="selectedSearchMethod"
          :options="searchMethodOptions"
          placeholder="Select search method"
          :highlightOnSelect="false"
          @update:modelValue="handleSearchMethodChange"
          size="large"
          v-tooltip.bottom="'Change the search method'"
        />
      </div>

      <Transition name="fade">
        <div
          class="tokenizer-option"
          v-if="selectedSearchMethod === 'Regular search'"
        >
          <span class="medium-text">Tokenizer</span>
          <Select
            checkmark
            class="custom-selector align-vertical-center"
            v-model="selectedTokenizer"
            :options="tokenizerOptions"
            placeholder="Select a tokenizer"
            :highlightOnSelect="false"
            @update:modelValue="handleTokenizerChange"
            size="large"
            v-tooltip.bottom="'Change the tokenizer'"
          />
        </div>
      </Transition>

      <div class="page-size">
        <span class="medium-text">Items per page</span>
        <Select
          checkmark
          class="custom-selector align-vertical-center"
          v-model="selectedPageSize"
          :options="pageSizes"
          :placeholder="selectedPageSize"
          :highlightOnSelect="false"
          :modelValue="selectedPageSize"
          @update:modelValue="handlePageSizeChange"
          size="large"
          v-tooltip.bottom="'Change the number of items per page'"
        />
      </div>
    </div>
  </div>

  <div class="row pagination-buttons">
    <button
      :disabled="currentPage === 1"
      @click="goToFirstPage"
      class="pagination-btn medium-text"
      v-tooltip.bottom="'Go to the first page'"
    >
      <i class="fas fa-angle-double-left"></i> <span class="button-text">First</span>
    </button>
    <button
      :disabled="currentPage === 1"
      @click="goToPreviousPage"
      class="pagination-btn medium-text"
      v-tooltip.bottom="'Go to the previous page'"
    >
      <i class="fas fa-chevron-left"></i> <span class="button-text">Previous</span>
    </button>

    <span class="current-page medium-text">
      {{ currentPage }} / {{ maxPages }}
    </span>

    <button
      :disabled="currentPage === maxPages"
      @click="goToNextPage"
      v-tooltip.bottom="'Go to the next page'"
      class="pagination-btn medium-text"
    >
      <i class="fas fa-chevron-right"></i> <span class="button-text">Next</span>
    </button>
    <button
      :disabled="currentPage === maxPages"
      @click="goToLastPage"
      v-tooltip.bottom="'Go to the last page'"
      class="pagination-btn medium-text"
    >
      <i class="fas fa-angle-double-right"></i> <span class="button-text">Last</span>
    </button>
  </div>
</template>

<script>
import Select from "primevue/select";

export default {
  name: "PaginationBar",
  components: {
    Select,
  },
  props: {
    currentPage: {
      type: Number,
      required: true,
    },
    maxPages: {
      type: Number,
      required: true,
    },
    yearOptions: {
      type: Object,
      required: true,
    },
  },
  data() {
    return {
      selectedPageSize: 10,
      pageSizes: [10, 20, 50],
      selectedYear: null,
      selectedSearchMethod: "Regular search",
      searchMethodOptions: ["Regular search", "Semantic search"],
      selectedTokenizer: "Standard",
      tokenizerOptions: ["Standard", "N-Gram"],
    };
  },
  methods: {
    handlePageSizeChange() {
      this.$emit("page-size-change", this.selectedPageSize);
    },
    handleYearChange() {
      this.$emit("year-change", this.selectedYear);
    },
    goToFirstPage() {
      this.$emit("go-to-page", 1);
    },
    goToPreviousPage() {
      if (this.currentPage > 1) {
        this.$emit("go-to-page", this.currentPage - 1);
      }
    },
    goToNextPage() {
      if (this.currentPage < this.maxPages) {
        this.$emit("go-to-page", this.currentPage + 1);
      }
    },
    goToLastPage() {
      this.$emit("go-to-page", this.maxPages);
    },
    handleSearchMethodChange() {
      this.$emit("search-method-change", this.selectedSearchMethod);
    },
    handleTokenizerChange() {
      this.$emit("tokenizer-change", this.selectedTokenizer);
      this.selectedYear = null;
    },
  },
};
</script>

<style scoped>
.pagination-bar-container {
  width: 100%;
  border: 1px solid rgba(234, 94, 19, 0.2);
  background: rgba(255, 255, 255, 0.05);
  padding: 1.5rem;
  margin-top: 2rem;
}

.pagination-bar-container .row {
  display: flex;
  gap: 1.25rem;
  flex-wrap: wrap;
  justify-content: space-between;
}

.custom-selector {
  border-radius: 0rem;
  height: 4rem;
  background: transparent;
  color: white !important;
  width: 100%;
}

.pagination-buttons {
  display: flex;
  align-items: center;
  justify-content: end;
  width: 100%;
  gap: 0.5rem;
  margin-top: 5rem;
}

.pagination-btn {
  background: transparent;
  height: 4rem;
  border: 1px solid rgba(234, 94, 19, 0.2);
  padding: 1rem;
  border-radius: 0rem;
  cursor: pointer;
  transition: background-color 0.3s ease, color 0.3s ease;
}

.pagination-btn:disabled {
  cursor: not-allowed;
  color: #666;
}

.pagination-btn:hover {
  background-color: rgba(234, 94, 19, 0.1);
}

.current-page {
  margin: 0 1rem;
}

.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.3s ease;
}

.fade-enter-from,
.fade-leave-to {
  opacity: 0;
}

.year-filter,
.search-method,
.tokenizer-option,
.page-size {
  display: flex;
  flex-direction: column;
  align-items: start;
  gap: 0.5rem;
  flex-grow: 1;
}

@media screen and (max-width: 992px) {
  .custom-selector {
    height: 3rem;
  }

  .pagination-btn {
    height: 3rem;
    padding: 0.5rem;
  }

  .pagination-buttons {
    margin-top: 3rem;
  }
}

@media screen and (max-width: 768px) {
  .pagination-buttons {
    margin-top: 2rem;
  }
}

@media screen and (max-width: 576px) {
  .pagination-btn .button-text {
    display: none;
  }

  .pagination-buttons {
    margin-top: 1rem;
  }
}
</style>
