describe("chorus.views.HdfsShowFileHeader", function() {
    beforeEach(function() {
        this.file = fixtures.hdfsEntryFile({ name: "myFile.txt" });
        this.view = new chorus.views.HdfsShowFileHeader({ model: this.file });
        this.view.render();
    });

    it ("has the file icon", function() {
        expect(this.view.$("img.icon").attr("src")).toBe("/images/workfiles/large/txt.png");
    });

    it("has the file name", function() {
        expect(this.view.$("h1").text()).toBe("myFile.txt");
        expect(this.view.$("h1").attr("title")).toBe("myFile.txt");
    });
});