class WorkspacesController < ApplicationController
  before_filter :load_workspace, :only => [:show]

  def index
    workspaces = Workspace.order("lower(name) ASC")
    workspaces = workspaces.active if params[:active]
    workspaces = workspaces.where(:owner_id => params[:user_id]) if params[:user_id]
    present workspaces.paginate(params.slice(:page, :per_page))
  end

  def create
    workspace = current_user.owned_workspaces.create!(params[:workspace])
    member = Membership.create!({:user => current_user, :workspace => workspace})
    present workspace, :status => :created
  end

  def show
    present @workspace
  end

  private

  def load_workspace
    @workspace = Workspace.find(params[:id])
  end
end
