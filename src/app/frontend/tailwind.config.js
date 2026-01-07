/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        rust: {
          DEFAULT: '#c7522a',
          light: '#e5633a',
          dark: '#a04220',
        },
        charcoal: {
          50: '#888888',
          100: '#505050',
          200: '#404040',
          300: '#3a3a3a',
          400: '#333333',
          500: '#2a2a2a',
          600: '#252525',
          700: '#1a1a1a',
          800: '#151515',
          900: '#0a0a0a',
        },
      },
      borderWidth: {
        '3': '3px',
      }
    },
  },
  plugins: [],
}

