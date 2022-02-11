package cn.cxg.entity;

import groovy.transform.ToString;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@NoArgsConstructor
@AllArgsConstructor
@Data
@ToString
public class UserBehavior {
    // 用户ID
    private Long userId;
    //商品ID
    private Long itemId;
    //种类ID
    private Integer categoryId;
    //数据类型
    private String behavior;
    //时间戳
    private Long dataTimestamp;

//    public UserBehavior() {
//    }
//
//    public UserBehavior(Long userId, Long itemId, Integer categoryId, String behavior, Long dataTimestamp) {
//        this.userId = userId;
//        this.itemId = itemId;
//        this.categoryId = categoryId;
//        this.behavior = behavior;
//        this.dataTimestamp = dataTimestamp;
//    }
//
//    public Long getUserId() {
//        return userId;
//    }
//
//    public void setUserId(Long userId) {
//        this.userId = userId;
//    }
//
//    public Long getItemId() {
//        return itemId;
//    }
//
//    public void setItemId(Long itemId) {
//        this.itemId = itemId;
//    }
//
//    public Integer getCategoryId() {
//        return categoryId;
//    }
//
//    public void setCategoryId(Integer categoryId) {
//        this.categoryId = categoryId;
//    }
//
//    public String getBehavior() {
//        return behavior;
//    }
//
//    public void setBehavior(String behavior) {
//        this.behavior = behavior;
//    }
//
//    public Long getTimestamp() {
//        return dataTimestamp;
//    }
//
//    public void setTimestamp(Long timestamp) {
//        this.dataTimestamp = timestamp;
//    }
//
//    @Override
//    public String toString() {
//        return "UserBehavior{" +
//                "userId=" + userId +
//                ", itemId=" + itemId +
//                ", categoryId=" + categoryId +
//                ", behavior='" + behavior + '\'' +
//                ", timestamp=" + dataTimestamp +
//                '}';
//    }
}
