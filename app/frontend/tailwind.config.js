/** @type {import('tailwindcss').Config} */
export default {
  content: [
    "./index.html",
    "./src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        renew: {
          primary: '#1a6b3a',
          secondary: '#f0f7f4',
          accent: '#2ea15f',
          dark: '#0f4023',
          light: '#e8f5ee',
        },
      },
    },
  },
  plugins: [],
}
