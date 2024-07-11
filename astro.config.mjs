import { defineConfig } from 'astro/config'
import icon from "astro-icon";

export default defineConfig({
  site: 'https://tsutomu-nagano.github.io',
  base: '/statibyte',
  integrations: [icon()],
})