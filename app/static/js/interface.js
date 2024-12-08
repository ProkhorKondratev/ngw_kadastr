class Interface {
    constructor() {
        this.toggleTheme();
    }

    toggleTheme() {
        const savedTheme = localStorage.getItem('selectedTheme') || 'light';
        const html = document.querySelector('html');
        html.setAttribute('data-bs-theme', savedTheme);

        const themeSwitch = document.querySelector('#toggleTheme');
        themeSwitch.checked = savedTheme === 'dark';

        themeSwitch.addEventListener('change', () => {
            const currentTheme = html.getAttribute('data-bs-theme');
            const newTheme = currentTheme === 'dark' ? 'light' : 'dark';

            html.setAttribute('data-bs-theme', newTheme);
            localStorage.setItem('selectedTheme', newTheme);
        });
    }
}