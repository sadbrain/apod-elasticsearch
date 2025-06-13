import { createApp } from "vue";
import App from "./App.vue";
import router from "./router";
import store from "./store";
import PrimeVue from "primevue/config";
import Tooltip from 'primevue/tooltip';
import Aura from "@primevue/themes/aura"
import axios from "axios";

axios.defaults.baseURL = 'http://localhost:8000'
const app = createApp(App);
app.use(store);
app.use(router, axios);
app.use(PrimeVue, {
    theme: {
        preset: Aura
    }
});
app.directive('tooltip', Tooltip);
app.mount("#app");
