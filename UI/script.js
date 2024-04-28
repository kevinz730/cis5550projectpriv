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

    fetch(`http://127.0.0.1:8001/search?query=${encodeURIComponent(searchTerm)}`)
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not OK');
            }

            return response.json();  // Parse the JSON in the response

        })
        .then(data => {
            console.log("Response data: ", data);  // Output the data to console
            if (!data.data || data.data.length === 0) {
                resultsDiv.innerHTML = '<p>No results found.</p>';
                return;
            }

        //     // Display the result
        //     const resultElement = document.createElement('div');
        //     resultElement.innerHTML = `
        //         <p>${data.result}</p>
        //     `;
        //     resultsDiv.appendChild(resultElement);
        // })
         // Display each result
         data.data.forEach(item => {
            const resultElement = document.createElement('div');
            resultElement.innerHTML = `
                <h4>URL: <a href="${item.url}">${item.url}</a></h4>
                <p>Score: ${item.score}</p>
            `;
            resultsDiv.appendChild(resultElement);
        });
    })
    .catch(error => {
        console.error('Failed to fetch data:', error);
        resultsDiv.innerHTML = '<p>Error loading results.</p>';
    });


    // // Mock results
    // const mockResults = [
    //     { title: 'Result 1', description: 'Description of result 1', url: '#' },
    //     { title: 'Result 2', description: 'Description of result 2', url: '#' },
    //     { title: 'Result 3', description: 'Description of result 3', url: '#' }
    // ];

    // // Display results
    // mockResults.forEach(result => {
    //     const resultElement = document.createElement('div');
    //     resultElement.innerHTML = `
    //         <h4><a href="${result.url}">${result.title}</a></h4>
    //         <p>${result.description}</p>
    //     `;
    //     resultsDiv.appendChild(resultElement);
    // });
}

