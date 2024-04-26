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

    // const mockResults = [
    //     { title: 'Result 1', description: 'Description of result 1', url: '#' },
    //     { title: 'Result 2', description: 'Description of result 2', url: '#' },
    //     { title: 'Result 3', description: 'Description of result 3', url: '#' }
    // ];

    // Fetch results from the server
    // fetch(`https://your-server.com/api/search?query=${encodeURIComponent(searchTerm)}`)
    // await //it's asynchronous
    fetch(`http://127.0.0.1:8001/search?query=${encodeURIComponent("Pennsylvania")}`, {mode: 'no-cors'})
        .then(response => {
            if (response === 200){
                throw new Error('Network response was not OK');
            }

            console.log("res " + response);
            console.log("res body " + response.body);
            console.log("res status " + response.status);
            console.log("res status txt " + response.statusText);
            console.log("res txt " + response.text);
            
            return response.json();
        })
        .then(data => {
            // Assuming the server returns an array of results
            if (data.length === 0) {
                resultsDiv.innerHTML = '<p>No results found.</p>';
                console.log("response" + response.body);
                return;
            }

    // Display each result
    data.forEach(result => {
        const resultElement = document.createElement('div');
        resultElement.innerHTML = `
            <h4><a href="${result.url}" target="_blank">${result.url}</a></h4>
            <p>${result.description}</p>
        `;
        resultsDiv.appendChild(resultElement);
    });
})
.catch(error => {
    console.error('Failed to fetch data:', error);
    resultsDiv.innerHTML = '<p>Error loading results.</p>';
});
}


// document.getElementById('searchForm').addEventListener('submit', function(event) {
//     event.preventDefault(); 

//     const searchTerm = document.getElementById('searchInput').value;
    
//     // fetch(`https://api.example.com/search?query=${encodeURIComponent(searchTerm)}`) 
//     fetch(`http://127.0.0.1:8001/view/pt-crawl`) 
//         .then(response => {
//             if (!response.ok) {
//                 throw new Error('Network response was not ok');
//             }
//             return response.json();
//         })
//         .then(data => {
//             const resultsDiv = document.getElementById('results');
//             resultsDiv.innerHTML = ''; // Clear previous results
//             if (data.results && data.results.length) {
//                 data.results.forEach(item => {
//                     const p = document.createElement('p');
//                     p.textContent = item.title; 
//                     resultsDiv.appendChild(p);
//                 });
//             } else {
//                 resultsDiv.innerHTML = '<p>No results found.</p>';
//             }
//         })
//         .catch(error => {
//             console.error('Failed to fetch data:', error);
//             document.getElementById('results').innerHTML = '<p>Error loading results.</p>';
//         });
// });

