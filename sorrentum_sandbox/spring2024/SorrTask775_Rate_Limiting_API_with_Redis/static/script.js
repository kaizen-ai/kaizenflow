async function update(value) {
    const select = document.getElementById('select');
    if (value === undefined) {
        do {
            value = Math.floor(Math.random() * 7);
        } while (value == select.value);
    }
    const response = await fetch('/api/' + value);
    const item = await response.json();
    select.value = value;
    const main = document.getElementById('main');
    main.textContent = null;
    for (const text of item['description']) {
        const p = document.createElement('p');
        p.textContent = text;
        main.appendChild(p);
    }
}
