# resource "aws_route_table" "private_route_table" {
#   vpc_id = var.vpc_id

#   route {
#     cidr_block = "0.0.0.0/0"
#     gateway_id = aws_nat_gateway.nat.id  # Replace with your NAT gateway or NAT instance ID
#   }

#   tags = {
#     Name = "private-route-table"
#   }
# }

# resource "aws_route_table_association" "private_subnet_association" {
#   subnet_id      = var.private_subnet_id
#   route_table_id = aws_route_table.private_route_table.id
# }