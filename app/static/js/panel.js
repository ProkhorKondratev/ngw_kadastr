class StatisticsPanel {
    constructor() {
        this.loadedElement = document.querySelector('#count-loaded');
        this.workingElement = document.querySelector('#count-working');
        this.successElement = document.querySelector('#count-success');
        this.errorElement = document.querySelector('#count-error');
        this.remainingElement = document.querySelector('#count-remaining');

        this.fetchStatistics()

        setInterval(() => {
            this.fetchStatistics();
        }, 8000);

    }

    updateStatistics(loaded, working, success, error, remaining) {
        this.loadedElement.textContent = loaded;
        this.workingElement.textContent = working;
        this.successElement.textContent = success;
        this.errorElement.textContent = error;
        this.remainingElement.textContent = remaining;
    }

    fetchStatistics() {
        fetch('/tasks/statistics')
            .then(response => response.json())
            .then(data =>
                this.updateStatistics(
                    data.loaded,
                    data.in_progress,
                    data.completed,
                    data.failed,
                    data.remaining
                )
            );
    }
}
