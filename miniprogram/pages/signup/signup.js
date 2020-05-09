const db = wx.cloud.database();
const app = getApp();
Page({
  data: {
    num: '',
    other: ''
  },
  //获取电话
  getNum(event) {
    console.log('获取输入的电话', event.detail.value)
    this.setData({
      num: event.detail.value
    })
  },
  //获取其他信息
  otherthing(event) {
    console.log('获取输入的其他信息', event.detail.value)
    this.setData({
      other: event.detail.value
    })
  },

  //注册
  signup() {
    let num = this.data.num
    let other = this.data.other
    console.log("点击了注册")
    //校验手机号
    if (num.length != 11) {
      wx.showToast({
        icon: 'none',
        title: '手机号码不正确',
      })
      return
    }
    //校验其他信息
    if (other && 0) {
      wx.showToast({
        icon: 'none',
        title: ' ',
      })
      return
    }
    console.log(app.globalData.nickName),
    //注册功能的实现
    db.collection('user').add({
      data: {
        nickName: app.globalData.nickName,
        num: num,
        other: other
      },
      success(res) {
        console.log('注册成功', res)
        wx.showToast({
          title: '注册成功',
        })
        wx.switchTab({
          url: '../first/first',
        }) 
      },
      fail(res) {
        console.log('注册失败', res)
      }
    })
  }
})