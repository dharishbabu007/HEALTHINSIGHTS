$(document).ready(function () {
    $('th.sortable').on('click', function () {
        const $th = $(this);
        const columnIndex = $th.index();
        const isAscending = $th.is('.sorted-asc');
        const sortDirection = isAscending ? -1 : 1;
        
        // Use closest() for cleaner DOM navigation
        const $table = $th.closest('table');
        const $rows = $table.find('tbody tr').get();
        
        // Sort rows with a single comparison function
        $rows.sort((a, b) => {
            const aValue = $(a).children('td').eq(columnIndex).text().trim();
            const bValue = $(b).children('td').eq(columnIndex).text().trim();
            
            // Try numeric comparison first
            const aNum = parseFloat(aValue);
            const bNum = parseFloat(bValue);
            
            if (!isNaN(aNum) && !isNaN(bNum)) {
                return (aNum - bNum) * sortDirection;
            }
            
            // Fall back to string comparison
            return aValue.localeCompare(bValue) * sortDirection;
        });
        
        // Re-append sorted rows
        const $tbody = $table.find('tbody');
        $rows.forEach(row => $tbody.append(row));
        
        // Update visual indicators
        $table.find('th.sortable').removeClass('sorted-asc sorted-desc');
        $th.addClass(isAscending ? 'sorted-desc' : 'sorted-asc');
        
        $table.find('td').removeClass('sorted')
            .filter(':nth-child(' + (columnIndex + 1) + ')')
            .addClass('sorted');
    });
});