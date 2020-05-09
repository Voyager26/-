// pages/select/select.js
Page({

  /**
   * 页面的初始数据
   */
  data: {
    singerList:[],
    name: [1,2,3],
    inputShowed: false,
    inputVal: "",
    loading: false,
    hideLoading: false,
  },

  gotoPage: function(event){
    let num = event.currentTarget.dataset.num;   //num为singerList数组的index
    let that = this
    //通过缓存的方式传输数据
    try {
      wx.setStorageSync('singer',this.data.singerList[num])
      console.log("缓存成功") 
    } catch (e) {
        console.log("缓存失败")
     }
  
    //跳转页面
    wx.navigateTo({
      url: '/pages/detail/detail'
    })
  },

  //搜索功能
  //能看不能用orz
  showInput: function () {
    this.setData({
        inputShowed: true
    });
  },
  hideInput: function () {
    this.setData({
        inputVal: "",
        inputShowed: false
    });
  },
  clearInput: function () {
    this.setData({
        inputVal: ""
    });
  },
  inputTyping: function (e) {
    this.setData({
        inputVal: e.detail.value
    });
  },

  /**
   * 生命周期函数--监听页面加载
   */
  onLoad: function(){

  },

  /**
   * 生命周期函数--监听页面初次渲染完成
   */
  onReady: function () {

  },

  /**
   * 生命周期函数--监听页面显示
   */
  onShow: function (options) {
    let that = this
    //loading图标
    this.setData({
      loading: true
    });
    setTimeout(() => {
      this.setData({
          hideLoading: true
      });
      setTimeout(() => {
          this.setData({
              loading: false,
              hideLoading: false,
          });
      }, 200);
    }, 1000);
    //从数据库singer里读取信息
    wx.cloud.database().collection("singer").get({
      success(res){
        console.log("请求成功",res)
        that.setData({
          singerList : res.data
        })
        this.setData({
          loading: false,
          hideLoading: true
        });
      },
      fail(res){
        console.log("请求失败",res)
      }
    })
  },

  /**
   * 生命周期函数--监听页面隐藏
   */
  onHide: function () {

  },

  /**
   * 生命周期函数--监听页面卸载
   */
  onUnload: function () {

  },

  /**
   * 页面相关事件处理函数--监听用户下拉动作
   */
  onPullDownRefresh: function () {

  },

  /**
   * 页面上拉触底事件的处理函数
   */
  onReachBottom: function () {

  },

  /**
   * 用户点击右上角分享
   */
  onShareAppMessage: function () {

  }
})