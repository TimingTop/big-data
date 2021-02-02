

## 真实的用户表

### 用户订单表

> order info   订单表
```
  id               订单id
  productId         产品id
  productTypeId     产品类型id
  createtime        订单创建时间       yyyyMMdd hhmmss
  amount             ? 商品数量
  paytype           支付类型
  paytime           支付时间
  paystatus         支付状态: 0=未支付，1=已支付， 2=已退款
  couponamount      优惠券数量
  totalamount       总金额    这张订单最大的金额
  refundamout       退款
  num               ?
  userid            用户id
```

### 商品表





### 商品类别表