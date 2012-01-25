(function(ns) {
    ns.models.Workspace = chorus.models.Base.extend({
        urlTemplate : "workspace/{{id}}",
        showUrlTemplate : "workspaces/{{id}}",
        entityType : "workspace",

        customIconUrl: function(options) {
            options = (options || {});
            return "/edc/workspace/" + this.get("id") + "/image?size=" + (options.size || "original");
        },

        defaultIconUrl: function() {
            if (this.get("active")) {
                return "/images/workspace-icon-large.png";
            } else {
                return "/images/workspace-archived-icon-large.png";
            }
        },

        owner: function() {
            this._owner = this._owner || new ns.models.User({
                id: this.get("ownerId"),
                firstName: this.get("ownerFirstName"),
                lastName: this.get("ownerLastName")
            });
            return this._owner;
        },

        sandbox: function() {
            if (this._sandbox) return this._sandbox;
            var sandboxInfo = this.get("sandboxInfo");
            if (sandboxInfo && sandboxInfo.sandboxId) {
                var attrs = _.extend({}, sandboxInfo, { id: sandboxInfo.sandboxId })
                delete attrs.sandboxId;
                return this._sandbox = new chorus.models.Sandbox(attrs);
            }
        },

        comments: function(){
            this._comments || (this._comments = new chorus.collections.CommentSet(this.get("latestCommentList")));
            return this._comments;
        },

        members: function(){
            if (!this._members) {
                this._members = new chorus.collections.MemberSet([], {workspaceId : this.get("id")})
                this._members.bind("saved", function() { this.trigger("change") }, this);
            }
            return this._members;
        },

        declareValidations : function(newAttrs) {
            this.require("name", newAttrs);
        },

        archiver: function() {
            return new ns.models.User({
                fullName: (this.get("archiverFirstName") + ' ' + this.get("archiverLastName")),
                userName: this.get("archiver")
            });
        },

        displayName : function() {
            return this.get("name");
        },

        displayShortName: function(length) {
            length = length || 20;

            var name = this.displayName() || "";
            return (name.length < length) ? name : name.slice(0,length) + "...";
        },

        imageUrl : function(options) {
            options = (options || {});
            return "/edc/workspace/" + this.get("id") + "/image?size=" + (options.size || "original");
        },

        picklistImageUrl : function() {
            return "/images/workspace-icon-small.png";
        },

        attrToLabel : {
            "name" : "workspace.validation.name"
        },

        truncatedSummary: function(length) {
            if (this.get("summary")) {
                return this.get("summary").substring(0, length);
            }
        },

        hasImage: function() {
            return this.get("iconId") != null;
        },

        isTruncated: function() {
            return this.get("summary") ? this.get("summary").length > 100 : false;
        },

        canRead : function() {
            return this._hasPermission(['admin', 'read']);
        },

        canComment : function() {
            return this._hasPermission(['admin', 'commenting']);
        },

        canUpdate : function() {
            return this._hasPermission(['admin', 'update']);
        },

        workspaceAdmin : function() {
            return this._hasPermission(['admin']);
        },

        _hasPermission : function(validPermissions) {
            return _.intersection(this.get("permission"), validPermissions).length > 0;
        }
    });
})(chorus);
