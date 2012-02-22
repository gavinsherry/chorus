describe("chorus.views.SqlWorkfileContentDetails", function() {
    beforeEach(function() {
        this.model = fixtures.workfile({ fileName: 'test.sql', content: "select * from foo" });
        this.model.workspace().set({
            sandboxInfo: {
                databaseId: '3',
                databaseName: "db",
                instanceId: '2',
                instanceName: "instance",
                sandboxId: "10001",
                schemaId: '4',
                schemaName: "schema"
            }});
        this.view = new chorus.views.SqlWorkfileContentDetails({ model: this.model })
        spyOn(this.view, 'runInExecutionSchema').andCallThrough();
        this.qtipElement = stubQtip()
    });

    describe("render", function() {
        beforeEach(function() {
            this.view.render();
        });

        it("shows the 'Run File' button", function() {
            expect(this.view.$('button.run_file')).toContainTranslation('workfile.content_details.run_file')
        });

        context("opening the Run File menu", function() {
            beforeEach(function() {
                this.view.$(".run_file").click()
            })

            it("shows the 'run in another' schema link in the menu", function() {
                expect(this.qtipElement).toContainTranslation("workfile.content_details.run_in_another_schema")
            });

            describe("when the workspace has been run in a schema other than its sandbox's schema", function() {
                beforeEach(function() {
                    _.extend(this.model.get("executionInfo"), {
                        instanceId: '51',
                        instanceName: "bob_the_instance",
                        databaseId: '52',
                        databaseName: "bar",
                        schemaId: '53',
                        schemaName: "wow"
                    });
                    this.view.render();
                    this.view.$(".run_file").click();
                });

                it("shows that schema's canonical name in the default 'run' link", function() {
                    var runLink = this.qtipElement.find(".run_default");
                    expect(runLink).toBe("a");
                    expect(runLink).toContainTranslation("workfile.content_details.run_in_last_schema", {
                        schemaName: this.model.executionSchema().canonicalName()
                    });
                });
            });

            describe("when the workspace has a sandbox, and hasn't been executed", function() {
                it("shows 'Run in the workspace sandbox'", function() {
                    var runLink = this.qtipElement.find(".run_default");
                    expect(runLink).toContainTranslation("workfile.content_details.run_workspace_sandbox")
                    expect(runLink).toBe("a");
                });
            });

            describe("when the workfile was last executed in its workspace's sandbox", function() {
                beforeEach(function() {
                    _.extend(this.model.get("versionInfo"), {
                        databaseId: '3',
                        databaseName: "db",
                        instanceId: '2',
                        instanceName: "instance",
                        schemaId: '4',
                        schemaName: "schema"
                    });
                    this.view.render();
                    this.view.$(".run_file").click();
                });

                it("shows 'Run in the workspace sandbox'", function() {
                    var runLink = this.qtipElement.find(".run_default");
                    expect(runLink).toContainTranslation("workfile.content_details.run_workspace_sandbox")
                    expect(runLink).toBe("a");
                });
            });

            describe("when the workspace does not have a sandbox", function() {
                beforeEach(function() {
                    spyOn(this.model.workspace(), 'sandbox');
                    this.view.render();
                    this.view.$(".run_file").click();
                });

                it("disables the 'run in sandbox' link", function() {
                    var runLink = this.qtipElement.find(".run_default");
                    expect(runLink).toContainTranslation("workfile.content_details.run_workspace_sandbox")
                    expect(runLink).toBe("span");
                });

                it("does nothing when the disabled span is clicked", function() {
                    spyOn(chorus.PageEvents, "broadcast").andCallThrough();
                    this.qtipElement.find(".run_default").click();
                    expect(chorus.PageEvents.broadcast).not.toHaveBeenCalled();
                });
            });

            context("clicking on 'Run in sandbox'", function() {
                beforeEach(function() {
                    spyOn(chorus.PageEvents, "broadcast").andCallThrough();
                    this.qtipElement.find('.run_default').click();
                });

                it("broadcasts the 'file:runCurrent' event on the view", function() {
                    expect(chorus.PageEvents.broadcast).toHaveBeenCalledWith("file:runCurrent");
                });
            })

            describe("clicking on 'Run in another schema'", function() {
                beforeEach(function() {
                    spyOn(chorus.dialogs.RunFileInSchema.prototype, "launchModal")
                    this.qtipElement.find('.run_other_schema').click();
                });

                it("launches the RunFileInSchema dialog", function() {
                    expect(chorus.dialogs.RunFileInSchema.prototype.launchModal).toHaveBeenCalled();
                })
            });
        })
    });

    describe("event handling", function() {
        describe("workfile:executed", function() {
            beforeEach(function() {
                spyOn(this.view, "render");
                spyOnEvent(this.view.model, "change")
                this.executionInfo = {
                    instanceId: '51',
                    instanceName: "ned",
                    databaseId: '52',
                    databaseName: "rob",
                    schemaId: '53',
                    schemaName: "louis"
                }

                chorus.PageEvents.broadcast("workfile:executed", this.model, this.executionInfo);
            });

            it("updates the execution info in the workfile", function() {
                expect(this.view.model.get("executionInfo")).toBe(this.executionInfo);
            })

            it("re-renders", function() {
                expect(this.view.render).toHaveBeenCalled();
            })

            it("does not trigger change on the model", function() {
                expect("change").not.toHaveBeenTriggeredOn(this.view.model);
            })
        })
    });
});
