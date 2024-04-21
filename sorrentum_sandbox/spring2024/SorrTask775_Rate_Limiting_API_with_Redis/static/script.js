async function api() {
    try {
        const response = await fetch('/api');
        if (response.ok) {
            const data = await response.json();
            console.log(data);
        } else {
            throw new Error(`${response.status} (${response.statusText})`);
        }
    } catch (error) {
        console.error(error);
    }
}
