import { defineConfig } from 'astro/config';
import icon from "astro-icon";
import node from "@astrojs/node";

// https://astro.build/config
export default defineConfig({
  site: 'https://tsutomu-nagano.github.io',
  base: '/statibyte',
  integrations: [icon()]
  // output: "server",
  // adapter: node({
  //   mode: "standalone"
  // })
});