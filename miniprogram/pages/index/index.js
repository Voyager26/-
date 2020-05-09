const app = getApp();
const db = wx.cloud.database();
Page({

  /**
   * 页面的初始数据
   */
  data: {
    hiddenButton: true,
    canIUse: wx.canIUse('button.open-type.getUserInfo'),
    openid: '',
    userInfo: null
  },
  
  onGotUserInfo: function (e) {
    // 定义调用云函数获取openid
    wx.cloud.callFunction({
      name: 'getUserid',
      complete: res => {
        console.log('openid--', res.result.openid)
        this.openid = res.result.openid
        app.globalData.nickName = e.detail.userInfo.nickName
        console.log(this.openid)
        db.collection('user').where({
          _openid: this.openid
        }).get({
          success: function (res) {
            var user = res.data[0]
            if (res.data.length != 0) {
              console.log(user.nickName)
              console.log('登陆成功')
              wx.showToast({
                title: '登陆成功',
                duration: 0,
                icon: 'success'
              })
              wx.switchTab({
                url: '../select/select',
              }) 
              wx.setStorageSync('user', user)
            } else {
              console.log("未注册")
              wx.navigateTo({
                url: '../signup/signup',
              })
            }
          }
        })
      }
    })
  },
  /**
   * 生命周期函数--监听页面加载
   */
  onLoad: function () {
    let _this = this
    //需要用户同意授权
    wx.getSetting({
      success: function (res) {
        if (res.authSetting['scope.userInfo']) {
          app.globalData.auth['scope.userInfo'] = true //将授权结果写入app.js里的全局变量
          wx.cloud.callFunction({
            name: 'get_setUserInfo',
            data: {
              getSelf: true
            },
            success: res => {
              if (res.errMsg == "cloud.callFunction:ok" && res.result) {
                //如果成功获取到，将用户资料写入app.js的全局变量
                console.log(res)
                app.globalData.userInfo = res.result.data.userData
                app.globalData.userId = res.result.data._id
                wx.switchTab({
                  url: '/pages/course'
                })
              } else {
                _this.setData({
                  hiddenButton: false
                })
                console.log("未注册")
              }
            },
            fail: err => {
              _this.setData({
                hiddenButton: false
              })
              //console.error("get_setUserInfo调用失败",err.errMsg)
            }
          })
        } else {
          _this.setData({
            hiddenButton: false
          })
          console.log("未授权")
        }
      },
      fail(err) {
        _this.setData({
          hiddenButton: false
        })
        console.error("wx.getSetting调用失败", err.errMsg)
      }
    })
  }
})