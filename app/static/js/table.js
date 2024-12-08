function getStatusPriority(status) {
    const priorities = {
        'PREPARING': -1,
        'SUCCESS': 2,
        'FAILED': 3,
        'ACCEPTED': 4,
        'STARTED': 5,
    };
    return priorities[status] || 0
}

class Table {
    constructor(selector) {
        this.selector = selector;
        this.table = null;
        this.refreshInterval = null;

        this.baseOptions = {
            deferRender: true,
            destroy: true,
            language: {
                url: '/static/libs/datatables/rus.json'
            },
            order: [[2, 'desc']],
        }

        this.taskOptions = {
            ajax: {
                url: '/tasks',
                dataSrc: 'tasks'
            },
            columns: [
                {data: 'id', title: 'ID', width: '5%'},
                {data: 'name', title: 'Участок', width: '15%'},
                {data: null, title: 'Статус', width: 'auto'},
                {data: 'added', title: 'Дата добавления', width: '10%'},
                {data: null, title: 'Действия', width: '5%', orderable: false}
            ],

            columnDefs: [
                {
                    targets: 2,
                    render: function (data, type, row) {
                        const task_kpt_status = row.kpt_status || {state: 'REJECTED'};
                        const task_kad_status = row.kad_status || {state: 'REJECTED'};

                        const display = `
                            <div class="d-flex flex-grow-1 flex-wrap gap-1 justify-content-start align-items-center">
                                <div>${Table.getStatus(task_kpt_status, 'Список кпт:')}</div>
                                <div>${Table.getStatus(task_kad_status, 'Кварталы:')}</div>
                            </div>
                        `

                        if (type === 'sort') {
                            const kptPriority = getStatusPriority(task_kpt_status.state);
                            const kadPriority = getStatusPriority(task_kad_status.state);
                            return Math.max(kptPriority, kadPriority);
                        }

                        return display;
                    }
                },
                {
                    targets: 3,
                    render: function (data, type, row) {
                        return Uploader.getDate(row.added)
                    }
                },
                {
                    targets: 4,
                    render: function (data, type, row) {
                        return `
                            <div class="btn-group">
                                <a onclick="Uploader.downloadFile('tasks', ${row.id}, this)" class="btn btn-sm btn-primary">${downloadSvg}</a>  
                                <button onclick="Uploader.deleteTask(${row.id}, this)" class="btn btn-sm btn-primary">${deleteSvg}</button>
                                <button onclick="Uploader.restartTask(${row.id}, this)" class="btn btn-sm btn-primary">${reloadSvg}</button>
                            </div>
                        `;
                    }
                }
            ],
            ...this.baseOptions
        }

        this.groupOptions = {
            ajax: {
                url: '/groups',
                dataSrc: 'groups'
            },

            columns: [
                {data: 'id', title: 'ID', width: '5%'},
                {data: 'name', title: 'Группа', width: '15%'},
                {data: 'tasks', title: 'Статистика', width: 'auto'},
                {data: 'added', title: 'Дата добавления', width: '10%'},
                {data: null, title: 'Действия', width: '5%', orderable: false}
            ],
            columnDefs: [
                {
                    targets: 2,
                    render: function (data, type, row) {
                        const display = `
                        <div class="d-flex flex-grow-1 flex-wrap gap-1 justify-content-start align-items-center">
                                <span class="badge text-bg-info">Загружено: ${row.statistics.loaded}</span>
                                <span class="badge 
                                    ${row.statistics.in_progress > 0 ? "text-bg-warning" : "text-bg-secondary"}">
                                    В обработке: ${row.statistics.in_progress}
                                </span>
                                <span class="badge text-bg-success">Обработаны: ${row.statistics.completed}</span>
                                <span class="badge text-bg-danger">С ошибкой: ${row.statistics.failed}</span>
                                <span class="badge text-bg-secondary">Осталось: ${row.statistics.remaining}</span>
                            </div>
                        `

                        if (type === 'sort') {
                            return row.statistics.in_progress
                        }

                        return display;
                    }
                },
                {
                    targets: 3,
                    render: function (data, type, row) {
                        return Uploader.getDate(row.added)
                    }
                },
                {
                    targets: 4,
                    render: function (data, type, row) {
                        return `
                                <div class="btn-group">
                                    <a onclick="Uploader.downloadFile('groups', ${row.id}, this)" class="btn btn-sm btn-primary">${downloadSvg}</a>  
                                    <button onclick="Uploader.deleteGroup(${row.id}, this)" class="btn btn-sm btn-primary">${deleteSvg}</button>
                                    <button onclick="Uploader.restartGroup(${row.id}, this)" class="btn btn-sm btn-primary">${reloadSvg}</button>
                                </div>
                            `;
                    }
                }
            ],
            ...this.baseOptions
        }

        this.initRefresh();
        this.initSwitcher();
    }

    initSwitcher() {
        const switcher = document.querySelector('#switchTable');
        let tableType = localStorage.getItem('tableType') || 'tasks';

        switcher.addEventListener('click', (e) => {
            tableType = e.target.checked ? 'groups' : 'tasks';
            this.stopRefresh();

            const table = document.querySelector('#table-space');
            table.classList.add('dt-fade-out');
            table.onanimationend = () => {
                table.classList.remove('dt-fade-out');
                this.initTable(tableType);
                this.initRefresh();
                localStorage.setItem('tableType', tableType);
            }
        })

        switcher.checked = tableType === 'groups';
        switcher.dispatchEvent(new Event('click'));
    }

    initTable(tableType = 'tasks') {
        if (tableType === 'tasks')
            this.table = new DataTable(this.selector, this.taskOptions);
        else if (tableType === 'groups')
            this.table = new DataTable(this.selector, this.groupOptions);
    }

    initRefresh() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
            this.refreshInterval = null;
        }
        this.refreshInterval = setInterval(() => {
            this.refreshTable();
        }, 8000);
    }

    stopRefresh() {
        if (this.refreshInterval) {
            clearInterval(this.refreshInterval);
            this.refreshInterval = null;
        }
    }

    refreshTable() {
        this.table.ajax.reload(null, false);
    }


    static getStatus(status, message = '') {
        const state = status.state;
        const error = status.error || '';
        const statuses = {
            'ACCEPTED': `<span class="badge text-bg-secondary">${message} Принято к исполнению</span>`,
            'STARTED': `<span class="badge text-bg-warning">${message} В обработке</span>`,
            'SUCCESS': `<span class="badge text-bg-success">${message} Готово</span>`,
            'FAILED': `<span class="badge text-bg-danger">${message} Ошибка</span> <span class="badge text-bg-danger">${error}</span>`,
            'CANCELLED': `<span class="badge text-bg-secondary">${message} Отменено</span>`,
            'REJECTED': ``,
        }

        return statuses[state] || statuses['REJECTED']
    }
}