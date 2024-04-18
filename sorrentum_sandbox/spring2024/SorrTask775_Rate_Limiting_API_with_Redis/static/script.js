async function api() {
    const response = await fetch('/api');
    const main = document.getElementById('main');
    const time = new Date().toLocaleTimeString();
    const status = response.status;
    const text = response.statusText;
    main.innerHTML += `<p>${time} - ${status} ${text}</p>`;
}