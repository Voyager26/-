const db = wx.cloud.database()
const time = db.collection('singer')
Page({
  data: {
    time_list: [{
        dis: 0
      },
      {
        dis: 0
      },
      {
        dis: 0
      },
      {
        dis: 0
      },
      {
        dis: 0
      },
      {
        dis: 0
      },
      {
        dis: 0
      },
    ],
    items: [],
    singer: {}
  },
  onLoad: function (options) {
    var that = this
    try {
      var value = wx.getStorageSync('singer') //将缓存的信息取出
      if (value) {
        // Do something with return value
        console.log("传输成功", value)
        this.singer = value
      }
    } catch (e) {
      // Do something when catch error
      console.log("传输失败", value)
    }
    var _this = this
    var time_list1 = this.data.time_list;
    console.log(this.singer)
    db.collection('singer').where({
        _id: this.singer._id
      })
      .get({
        success: res => {
          console.log(res.data)
          var freetime = res.data[0].freetime
          console.log(freetime)
          for (var i = 0; i < 7; i++) {
            time_list1[i].dis = freetime[i]
          }
          var items = that.data.items
          if (time_list1[0].dis == 1) {
            console.log("in the if")
            var item = {
              name: '8-10',
              value: '8：00至10：00',
              num: 0
            }
            items.push(item)
            that.setData({items});
          }
          if (time_list1[1].dis == 1) {
            var item = {
              name: '10-12',
              value: '10：00至12：00',
              num: 1
            }
            items.push(item)
            that.setData({items});
          }
          if (time_list1[2].dis == 1) {
            var item = {
              name: '12-14',
              value: '12：00至14：00',
              num: 2
            }
            items.push(item)
            that.setData({items});
          }
          if (time_list1[3].dis == 1) {
            var item = {
              name: '14-16',
              value: '14：00至16：00',
              num: 3
            }
            items.push(item)
            that.setData({items});
          }
          if (time_list1[4].dis == 1) {
            var item = {
              name: '16-18',
              value: '16：00至18：00',
              num: 4
            }
            items.push(item)
            that.setData({items});
          }
          if (time_list1[5].dis == 1) {
            var item = {
              name: '18-20',
              value: '18：00至20：00',
              num: 5
            }
            items.push(item)
            that.setData({items});
          }
          if (time_list1[6].dis == 1) {
            var item = {
              name: '20-22',
              value: '20：00至22：00',
              num: 6
            }
            items.push(item)
            that.setData({items});
          }
          console.log("1",this.data.items)
        }
      })

  },
  toSetTimeTable: function () {
    db.collection('singer').where({
        _id: this.singer._id
      })
      .get({
        success: res => {
          console.log(res.data)
          var freeTime = res.data[0].freetime
          if (freeTime[time] == 1) {
            freeTime[time] = 2
            console.log(freeTime)
            //！主要问题在这里，无法将课程信息同步到数据库
            wx.cloud.callFunction({
              // 云函数名称
              name: 'updateTeacherFreetime',
              data: {
                'freetime': 'freeTime'
              },
              success(res) {
                console.log("调用成功")
                wx.showToast({
                  title: '预约成功',
                  duration: 800,
                  icon: 'none'
                })
                wx.switchTab({ //跳转到页面
                  url: '../my/my' //跳转到的页面地址
                })
              },
              fail(res){
                console.log("调用失败")
              }
            })
          }
        }
      })


  },
  radioChange: function (e) {
    console.log('radio发生change事件，携带value值为：', e.detail.value)
    this.radio = e.detail.value
  }
})