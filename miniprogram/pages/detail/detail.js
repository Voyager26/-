// pages/detail/detail.js
Page({

  /**
   * 页面的初始数据
   */
  data: {
    singer: {},
    dis:false
  },

  /*！！！重要！！！！点击按钮时，实现跳转到zyh页面的功能
  （应该还要实现传参到zyh的页面，数据已经在缓存里了，打开调试器“storage”可见数据）
  （缓存已经存好，不用再次存入缓存了，跳转到新的页面直接取出就好）*/
  toSelectTime: function () { //再点击“选课”按钮时执行这个方法、
    var flag = 1
    for (var i = 0; i < 7; i++) {
      if (this.singer.freetime[i] == 1) {
        flag = 0;
      }
    }
    if (flag) {
      console.log('时间已满')
      this.dis = true
      wx.showToast({
        title: '时间已预约满，请更换singer',
        duration: 800,
        icon: 'none'
      })
    }
    else{
    wx.navigateTo({ //跳转到页面
      url: '../timeSelect/timeSelect' //跳转到的页面地址
    })
  }
  },

  /**
   * 生命周期函数--监听页面加载
   */
  onLoad: function (options) {
    var that = this
    try {
      var value = wx.getStorageSync('singer') //将缓存的信息取出
      var flag = 1 //判断是否有时间
      if (value) {
        // Do something with return value
        console.log("传输成功", value)
        that.setData({
          singer: value
        })
        this.singer = value
        console.log("singer", this.singer.freetime)
        for (var i = 0; i < 7; i++) {
          if (this.singer.freetime[i] == 1) {
            flag = 0;
          }
        }
        if (flag) {
          console.log('时间已约满')
          this.dis = true
          wx.showToast({
            title: '时间已预约满，请更换singer',
            duration: 800,
            icon: 'none'
          })
        }
      }
    } catch (e) {
      // Do something when catch error
      console.log("传输失败", value)
    }

  },

  /**
   * 生命周期函数--监听页面初次渲染完成
   */
  onReady: function () {

  },

  /**
   * 生命周期函数--监听页面显示
   */
  onShow: function () {

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