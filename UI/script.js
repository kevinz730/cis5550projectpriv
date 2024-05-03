// document.getElementById('searchForm').addEventListener('submit', function(event) {
//     event.preventDefault();
//     const searchTerm = document.getElementById('searchInput').value;
//     displayResults(searchTerm);
// });

// function displayResults(searchTerm) {
//     const resultsDiv = document.getElementById('results');
//     resultsDiv.innerHTML = '';  // Clear previous results
//     if (!searchTerm) {
//         resultsDiv.innerHTML = '<p>Please enter a search term.</p>';
//         return;
//     }

//     fetch(`http://127.0.0.1:8001/search?query=${encodeURIComponent(searchTerm)}`)
//         .then(response => {
//             if (!response.ok) {
//                 throw new Error('Network response was not OK');
//             }

//             return response.json();  // Parse the JSON in the response

//         })
//         .then(data => {
//             console.log("Response data: ", data);  // Output the data to console
//             if (!data.data || data.data.length === 0) {
//                 resultsDiv.innerHTML = '<p>No results found.</p>';
//                 return;
//             }

//         //     // Display the result
//         //     const resultElement = document.createElement('div');
//         //     resultElement.innerHTML = `
//         //         <p>${data.result}</p>
//         //     `;
//         //     resultsDiv.appendChild(resultElement);
//         // })
//          // Display each result
//          data.data.forEach(item => {
//             const resultElement = document.createElement('div');
//             resultElement.innerHTML = `
//                 <h4>URL: <a href="${item.url}">${item.url}</a></h4>
//                 <p>Score: ${item.score}</p>
//             `;
//             resultsDiv.appendChild(resultElement);
//         });
//     })
//     .catch(error => {
//         console.error('Failed to fetch data:', error);
//         resultsDiv.innerHTML = '<p>Error loading results.</p>';
//     });


//     // // Mock results
//     // const mockResults = [
//     //     { title: 'Result 1', description: 'Description of result 1', url: '#' },
//     //     { title: 'Result 2', description: 'Description of result 2', url: '#' },
//     //     { title: 'Result 3', description: 'Description of result 3', url: '#' }
//     // ];

//     // // Display results
//     // mockResults.forEach(result => {
//     //     const resultElement = document.createElement('div');
//     //     resultElement.innerHTML = `
//     //         <h4><a href="${result.url}">${result.title}</a></h4>
//     //         <p>${result.description}</p>
//     //     `;
//     //     resultsDiv.appendChild(resultElement);
//     // });
// }



document.getElementById('searchForm').addEventListener('submit', function(event) {
    event.preventDefault();
    const searchTerm = document.getElementById('searchInput').value;
    displayResults(searchTerm, 0);  // Initial page index is 0
});

function displayResults(searchTerm, pageIndex) {
    const resultsDiv = document.getElementById('results');
    const paginationDiv = document.querySelector('.pagination');
    resultsDiv.innerHTML = '';  // Clear previous results
    paginationDiv.innerHTML = '';  // Clear previous pagination

    if (!searchTerm) {
        resultsDiv.innerHTML = '<p>Please enter a search term.</p>';
        return;
    }

    fetch(`http://127.0.0.1:8006/search?query=${encodeURIComponent(searchTerm)}&page=${pageIndex}`)
        .then(response => {
            if (!response.ok) {
                throw new Error('Network response was not OK');
            }
            return response.json();  // Parse the JSON in the response
        })
        .then(data => {
            if (!data.data || data.data.length === 0) {
                resultsDiv.innerHTML = '<p>No results found.</p>';
                return;
            }

            // Display each result
            data.data.forEach(item => {
                const resultElement = document.createElement('div');
                resultElement.innerHTML = `
                    <h4>URL: <a href="${item.url}">${item.url}</a></h4>
                    <p>Score: ${item.score}</p>
                `;
                resultsDiv.appendChild(resultElement);
            });

            // Pagination controls
            if (pageIndex > 0) {
                const prevBtn = document.createElement('button');
                prevBtn.textContent = 'Previous';
                prevBtn.className = 'pagination-btn';
                prevBtn.onclick = () => displayResults(searchTerm, pageIndex - 1);
                paginationDiv.appendChild(prevBtn);
            }
            if (data.data.length === 10) {  // Assuming 10 results per page
                const nextBtn = document.createElement('button');
                nextBtn.textContent = 'Next';
                nextBtn.className = 'pagination-btn';
                nextBtn.onclick = () => displayResults(searchTerm, pageIndex + 1);
                paginationDiv.appendChild(nextBtn);
            }
        })
        .catch(error => {
            console.error('Failed to fetch data:', error);
            resultsDiv.innerHTML = '<p>Error loading results.</p>';
        });
}

// Optional: Add .pagination div in your HTML for pagination controls.


// function displayResults(searchTerm, pageIndex) {
//     const resultsDiv = document.getElementById('results');
//     const paginationDiv = document.querySelector('.pagination');
//     resultsDiv.innerHTML = '';  // Clear previous results
//     paginationDiv.innerHTML = '';  // Clear previous pagination buttons

//     if (!searchTerm) {
//         resultsDiv.innerHTML = '<p>Please enter a search term.</p>';
//         return;
//     }

//     fetch(`http://127.0.0.1:8001/search?query=${encodeURIComponent(searchTerm)}&page=${pageIndex}`)
//         .then(response => response.json())
//         .then(data => {
//             if (!data || data.length === 0) {
//                 resultsDiv.innerHTML = '<p>No results found.</p>';
//                 return;
//             }

//             // Display each result
//             data.data.forEach(item => {
//                 const resultElement = document.createElement('div');
//                 resultElement.innerHTML = `<h4>URL: <a href="${item.url}">${item.url}</a></h4><p>Score: ${item.score}</p>`;
//                 resultsDiv.appendChild(resultElement);
//             });

//             // Add pagination buttons if necessary
//             const isLastPage = data.length < 10;  // Check if it's potentially the last page
//             if (pageIndex > 0) {
//                 const prevBtn = document.createElement('button');
//                 prevBtn.textContent = 'Previous';
//                 prevBtn.className = 'pagination-btn';
//                 prevBtn.onclick = () => displayResults(searchTerm, pageIndex - 1);
//                 paginationDiv.appendChild(prevBtn);
//             }
//             if (!isLastPage) {
//                 const nextBtn = document.createElement('button');
//                 nextBtn.textContent = 'Next';
//                 nextBtn.className = 'pagination-btn';
//                 nextBtn.onclick = () => displayResults(searchTerm, pageIndex + 1);
//                 paginationDiv.appendChild(nextBtn);
//             }
//         })
//         .catch(error => {
//             console.error('Error fetching data:', error);
//             resultsDiv.innerHTML = '<p>Error loading results.</p>';
//         });
// }
