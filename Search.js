// File: Search.js
// Replacing bitwise NOT operator with indexOf check.

function search(query) {
    let results = [];
    if (query && items) {
        results = items.filter(item => item.name.indexOf(query) !== -1);
    }
    return results;
}