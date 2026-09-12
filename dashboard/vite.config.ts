import { defineConfig } from 'vite'
import preact from '@preact/preset-vite'

export default defineConfig({
  plugins: [preact()],
  base: './',
  build: {
    outDir: '../nestor-server/_dashboard',
    emptyOutDir: true,
  },
  server: {
    proxy: {
      '/admin': 'http://127.0.0.1:9190',
      '/ready': 'http://127.0.0.1:9190',
      '/health': 'http://127.0.0.1:9190',
    },
  },
})
