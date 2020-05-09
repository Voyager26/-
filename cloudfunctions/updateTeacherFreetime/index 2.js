// 云函数入口文件
const cloud = require('wx-server-sdk')
 
cloud.init({
  env: "xbdcloud-5gq7l"
})
 
const db = cloud.database()
const _ = db.command
 
// 云函数入口函数
exports.main = async (event, context) => {
  try {
    return await db.collection('teacher').doc(event.this.teacher._id).set({
    data:{
      'freetime': 'event.freeTime'
    }
  })
  } catch (e) {
    console.error(e)
  }
}