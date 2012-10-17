describe("chorus.dialogs.KaggleInsertDatasetSchemaPicker", function() {
    var dialog, datasets, datasetModels;
    beforeEach(function() {
        stubModals();
        dialog = new chorus.dialogs.KaggleInsertDatasetSchema({ workspaceId : "33" });
        datasets = new chorus.collections.WorkspaceDatasetSet([], {workspaceId: "33" });
        datasetModels = [
                            newFixtures.workspaceDataset.sandboxTable({ objectName: "A", columns: 42, id: "REAL_ID" }),
                            newFixtures.workspaceDataset.chorusView({ objectName: "B", columns: 666, id: "AGENT_SMITH" })
                        ];
    });

    describe("#render", function() {
        var options;
        beforeEach(function() {
            options = { order: "objectName" };
            dialog.launchModal();
        });

        it("fetches the results sorted by objectName", function() {
            var url = this.server.lastFetch().url;
            var urlParams = _.extend({}, options);
            urlParams.order = "object_name";
            expect(url).toHaveUrlPath("/workspaces/33/datasets");
            expect(url).toContainQueryParams(urlParams);
        });

        describe("when the fetch completes", function() {
            beforeEach(function() {
                this.server.completeFetchFor(datasets, datasetModels, options);
                spyOn(chorus.dialogs.PreviewColumns.prototype, 'render').andCallThrough();
            });

            it("shows the correct title", function() {
                expect(dialog.$("h1")).toContainTranslation("kaggle.pick_datasets");
            });

            it("shows the correct search help", function() {
                expect(dialog.$("input.chorus_search").attr("placeholder")).toMatchTranslation("dataset.dialog.search_table");
            });

            it("shows the correct item count label", function() {
                expect(dialog.$(".count")).toContainTranslation("entity.name.Table", { count: 2 });
            });

            it("shows the correct button name", function() {
                expect(dialog.$("button.submit")).toContainTranslation("kaggle.datasets_select");
            });

            it("has multiSelection", function() {
                expect(dialog.multiSelection).toBeTruthy();
            });

            it("has serverside search", function() {
                expect(dialog.serverSideSearch).toBeTruthy();
            });

            it("shows all associated datasets", function() {
                expect(_.pluck(dialog.collection.models, "objectName")).toEqual(_.pluck(datasetModels, "objectName"));
            });

            it("shows a Preview Columns link for each dataset", function() {
                expect(dialog.$(".items li:eq(0) a.preview_columns")).toContainTranslation("dataset.manage_join_tables.preview_columns");
                expect(dialog.$(".items li:eq(1) a.preview_columns")).toContainTranslation("dataset.manage_join_tables.preview_columns");
            });

            it("has the correct id, not the CID", function() {
                expect(dialog.$(".items li:eq(0)").data("id")).toBe("REAL_ID");
                expect(dialog.$(".items li:eq(1)").data("id")).toBe("AGENT_SMITH");
            });

            it("shows the preview columns submodal with the appropriate dataset when you click the link", function() {
                dialog.$(".items li:eq(0) a.preview_columns").click();
                expect(chorus.dialogs.PreviewColumns.prototype.render).toHaveBeenCalled();
                var previewColumnsDialog = chorus.dialogs.PreviewColumns.prototype.render.mostRecentCall.object;
                expect(previewColumnsDialog.title).toBe(dialog.title);
                expect(previewColumnsDialog.model.get("id")).toEqual(datasetModels[0].get("id"));
            });

            it("shows the number of columns in each dataset", function() {
                expect(dialog.$(".items li:eq(0) .column_count")).toContainTranslation("dataset.column_count", {count: 42});
                expect(dialog.$(".items li:eq(1) .column_count")).toContainTranslation("dataset.column_count", {count: 666});
            });
        });

        context("when a dataset has no column count (or is undefined)", function() {
            beforeEach(function() {
                datasetModels = [
                                    newFixtures.workspaceDataset.sandboxTable({ objectName: "A", columns: null, id: "NOBODY" }),
                                    newFixtures.workspaceDataset.chorusView({ objectName: "B", columns: undefined, id: "NONE" })
                                ];
                datasets = new chorus.collections.WorkspaceDatasetSet([], { workspaceId: "33" });
                this.server.completeFetchFor(datasets, datasetModels, options);
            });

            it("doesn't show column count", function() {
                expect(dialog.$("li:eq(0) span.column_count")).not.toExist();
                expect(dialog.$("li:eq(1) span.column_count")).not.toExist();
            });
        });
    });
});