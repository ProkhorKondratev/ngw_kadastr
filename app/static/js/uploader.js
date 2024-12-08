class Uploader {
    constructor() {
        this._files = [];
        this._name = ''

        this.allowedTypes = [
            'zip',
            'geojson',
        ];

        this.dropZone = document.querySelector('#dropZone');
        this.selectedFiles = document.querySelector('#selectedFiles');

        this.initUploader();
    }

    initUploader() {
        if (this.dropZone) {
            this.setListeners()
            this.setOnClick()
            this.initModal()
        } else {
            console.error('Для работы Drag&Drop необходимо наличие элемента с id="dropZone"')
        }
    }

    setListeners() {
        this.dropZone.ondragenter = (e) => {
            e.preventDefault();
            this.dropZone.classList.add('hover');
        }

        this.dropZone.ondragover = (e) => {
            e.preventDefault();
            this.dropZone.classList.add('hover');
        }

        this.dropZone.ondragleave = (e) => {
            e.preventDefault();
            let relatedTarget = e.relatedTarget;
            if (!this.dropZone.contains(relatedTarget)) {
                this.dropZone.classList.remove('hover');
            }
        }

        this.dropZone.ondrop = (e) => {
            e.preventDefault();
            this.dropZone.classList.remove('hover');
            this.files = Array.from(e.dataTransfer.items).map(item => item.getAsFile());
        }
    }

    setOnClick() {
        this.dropZone.onclick = (e) => {
            if (e.target === this.dropZone) {
                const input = document.createElement('input');
                input.type = 'file';
                input.multiple = true;
                input.click();

                input.onchange = (e) => {
                    e.preventDefault();
                    this.dropZone.classList.remove('hover');
                    this.files = Array.from(e.target.files);
                }
            }
        }
    }

    get name() {
        return this._name;
    }

    set name(name) {
        const inputName = document.querySelector('#inputName');
        inputName.value = name;
        this._name = name.toString();
    }

    get files() {
        return this._files;
    }

    set files(files) {
        this._files = files.filter((item) => {
            const ext = item.name.split('.').pop();
            return this.allowedTypes.includes(ext);
        })

        const createFileBlock = (file) => {
            const fileBlock = document.createElement('div');
            fileBlock.className = 'card file-block';
            fileBlock.innerHTML = `
                <div class="card-body">
                    <p class="card-text">${file.name}</p>
                    <button class="btn btn-danger">${deleteSvg}</button>
                </div>
            `;

            fileBlock.querySelector('button').onclick = () => {
                this._files = this._files.filter(f => f !== file);
                fileBlock.remove();
            }

            return fileBlock;
        }

        this.selectedFiles.innerHTML = '';
        for (const file of this._files) this.selectedFiles.appendChild(createFileBlock(file));
    }

    async upload() {
        if (this.files.length === 0) {
            Uploader.showAlert('Файлы не выбраны!', 'warning');
            return;
        }

        const formData = new FormData();
        this.files.forEach(file => formData.append('files', file));
        formData.append('name', this.name);

        const response = await fetch('/run_tasks', {
            method: 'POST',
            body: formData,
        });

        return await response.json();
    }

    initModal() {
        const modal = new bootstrap.Modal(document.getElementById('exampleModal'));

        modal._element.addEventListener('hide.bs.modal', () => {
            this.files = [];
            this.name = '';
        });

        const nameInput = document.querySelector('#inputName');
        nameInput.addEventListener('input', (e) => {
            this.name = e.target.value;
        });

        const loadingSpinner = document.querySelector('#loading-spinner');

        document.querySelector('#btn-upload').addEventListener('click', async (e) => {
            e.target.disabled = true;
            loadingSpinner.style.opacity = '1';
            const data = await this.upload();

            e.target.disabled = false;
            loadingSpinner.style.opacity = '0';

            if (data) {
                if (data.errors?.length !== 0) Uploader.showAlert(data.errors.join('<br>'), 'warning')
                else Uploader.showAlert(data.message);

                modal.hide();
                table.refreshTable();
            }
        })
    }

    static showAlert(message, type = 'success') {
        const alert = document.createElement('div');
        alert.className = `alert alert-${type} alert-dismissible fade show`;
        alert.setAttribute('role', 'alert');
        alert.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
        `;
        alert.style.opacity = '0';
        alert.style.transition = 'opacity 0.5s';

        document.querySelector('.alert-wrapper').prepend(alert);

        setTimeout(() => alert.style.opacity = '1', 100);

        setTimeout(() => {
            alert.style.opacity = '0';
            alert.addEventListener('transitionend', function handler() {
                alert.remove();
                alert.removeEventListener('transitionend', handler);
            });
        }, 3000);
    }

    static deleteOrRestart(path, method, id, element = null, actionErrorText = 'Ошибка!') {
        if (element) element.disabled = true;
        table.stopRefresh()

        let innerHtml = element.innerHTML;
        element.innerHTML = `<div class="spinner-border" role="status" style="width: 16px; height: 16px"></div>`;

        fetch(`/${path}/${id}/${method}`, {
            method: method === 'delete' ? 'DELETE' : 'POST',
        })
            .then(response => response.json())
            .then(data => {
                Uploader.showAlert(data.message);
                table.refreshTable();
            })
            .catch(error => {
                Uploader.showAlert(`${actionErrorText}`, 'danger');
                console.error(error);
            })
            .finally(() => {
                element.innerHTML = innerHtml;
                element.disabled = false;
                table.initRefresh();
            });
    }

    static downloadFile(type, id, element) {
        if (element) element.disabled = true;
        table.stopRefresh()

        let innerHtml = element.innerHTML;
        element.innerHTML = `<div class="spinner-border" role="status" style="width: 16px; height: 16px"></div>`;

        try {
            const a = document.createElement('a');
            a.href = `/${type}/${id}/download`;
            a.click();
        } catch (error) {
            Uploader.showAlert('Ошибка при скачивании файла!', 'danger');
            console.error(error);
        } finally {
            element.innerHTML = innerHtml;
            element.disabled = false;
            table.initRefresh();
        }
    }

    static deleteGroup(id, element = null) {
        this.deleteOrRestart('groups', 'delete', id, element, 'Ошибка при удалении группы!');
    }

    static deleteTask(id, element = null) {
        this.deleteOrRestart('tasks', 'delete', id, element, 'Ошибка при удалении задачи!');
    }

    static restartTask(id, element = null) {
        this.deleteOrRestart('tasks', 'restart', id, element, 'Ошибка при перезапуске задачи!');
    }

    static restartGroup(id, element = null) {
        this.deleteOrRestart('groups', 'restart', id, element, 'Ошибка при перезапуске группы!');
    }

    static getDate(date) {
        return new Date(date).toLocaleString('ru', {
            year: 'numeric',
            month: 'numeric',
            day: 'numeric',
            hour: 'numeric',
            minute: 'numeric'
        });
    }

    // static downloadFile(id, element) {
    //     element.disabled = true;
    //     fetch(`/download/${id}`)
    //         .then(response => response.blob())
    //         .then(blob => {
    //             const url = window.URL.createObjectURL(blob);
    //             const a = document.createElement('a');
    //             a.href = url;
    //             a.download = `task_${id}.zip`;
    //             document.body.appendChild(a);
    //             a.click();
    //             a.remove();
    //         })
    //         .catch(error => {
    //             Uploader.showAlert('Ошибка при скачивании файла!', 'danger');
    //             console.error(error);
    //         })
    //         .finally(() => element.disabled = false);
    // }
}