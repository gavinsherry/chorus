require 'models/user'

class UsersController < ApplicationController
  before_filter :load_user, :only => [:show, :update, :destroy]
  before_filter :require_admin, :only => [:create, :destroy, :ldap]
  before_filter :require_not_current_user, :only => [:destroy]
  before_filter :require_admin_or_referenced_user, :only => :update

  wrap_parameters :exclude => []

  def index
    present paginate(User.order(params[:order]))
  end

  def show
    present @user
  end

  def create
    user = User.new
    user.attributes = params[:user]
    user.admin = params[:user][:admin]
    User.transaction do
      user.save!

      Events::UserAdded.by(current_user).add(:new_user => user)
    end

    present user, :status => :created
  end

  def update
    @user.attributes = params[:user]
    @user.admin = params[:user][:admin] if current_user.admin?
    @user.save!
    present @user
  end

  def destroy
    @user.destroy
    render :json => {}
  end

  def ldap
    users = LdapClient.search(params[:username]).map do |userJson|
      User.new userJson
    end
    present users
  end

  private

  def load_user
    @user = User.find(params[:id])
  end

  def require_not_current_user
    render_forbidden if current_user.id == @user.id
  end
end
