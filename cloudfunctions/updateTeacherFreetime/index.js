// 云函数入口文件
const cloud = require('wx-server-sdk')
 
cloud.init({
  env: "xbdcloud-5gq7l"
})
 
const db = cloud.database()
 
// 云函数入口函数
exports.main = async (event, context) => {
  const singerCollection = db.collection('singer')
  return await singerCollection.where({
      _id:event._id
  }).update({
    data:{
      freetime: event.freetime
    }
  })
}