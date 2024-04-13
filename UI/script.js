document.getElementById('searchForm').addEventListener('submit', function(event) {
    event.preventDefault();
    const searchTerm = document.getElementById('searchInput').value;
    displayResults(searchTerm);
});

function displayResults(searchTerm) {
    const resultsDiv = document.getElementById('results');
    resultsDiv.innerHTML = '';  // Clear previous results
    if (!searchTerm) {
        resultsDiv.innerHTML = '<p>Please enter a search term.</p>';
        return;
    }

    // Mock results
    const mockResults = [
        { title: 'Result 1', description: 'Description of result 1', url: '#' },
        { title: 'Result 2', description: 'Description of result 2', url: '#' },
        { title: 'Result 3', description: 'Description of result 3', url: '#' }
    ];

    // Display results
    mockResults.forEach(result => {
        const resultElement = document.createElement('div');
        resultElement.innerHTML = `
            <h4><a href="${result.url}">${result.title}</a></h4>
            <p>${result.description}</p>
        `;
        resultsDiv.appendChild(resultElement);
    });
}
