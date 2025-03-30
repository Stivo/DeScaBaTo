
export function formatBytes(bytes) {
    if(bytes == 0) return '0 Bytes';
    var k = 1024,
       dm = 2,
       sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'],
       i = Math.floor(Math.log(bytes) / Math.log(k));
   return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

export function byteFormatter(cell, formatterParams) {
    var bytes = cell.getValue();
    return formatBytes(bytes);
}

export function formatTimestamp(timestamp) {
    const date = new Date(timestamp);
    const day = String(date.getDate()).padStart(2, '0');
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const year = date.getFullYear();
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    const seconds = String(date.getSeconds()).padStart(2, '0');
    return `${day}.${month}.${year} ${hours}:${minutes}:${seconds}`;
}

export function timestampFormatter(cell, formatterParams) {
    var bytes = cell.getValue();
    return formatTimestamp(bytes);
}
