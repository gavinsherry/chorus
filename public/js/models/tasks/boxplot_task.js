chorus.models.BoxplotTask = chorus.models.ChartTask.extend({
    chartType: "boxplot",

    beforeSave: function() {
        this._super("beforeSave");
        this.set({
            'chart[xAxis]': this.get("xAxis"),
            'chart[yAxis]': this.get("yAxis")
        });
    }
});
