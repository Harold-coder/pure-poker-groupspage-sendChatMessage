const AWS = require('aws-sdk');
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const groupsTableName = process.env.GROUPS_TABLE;
const connectionsTableName = process.env.CONNECTIONS_TABLE;

exports.handler = async (event) => {
    const { groupId, userId, message } = JSON.parse(event.body);
    const connectionId = event.requestContext.connectionId;

    console.log("Groupid:", groupId);
    console.log("userId:", userId);
    console.log("message:", message);


    // Validate user is part of the group and is in the chat
    const groupResponse = await dynamoDb.get({
        TableName: groupsTableName,
        Key: { groupId },
    }).promise();

    const group = groupResponse.Item;
    if (!group) {
        return { statusCode: 404, body: JSON.stringify({ message: "Group not found." }) };
    }

    if (!group.membersList.includes(userId)) {
        return { statusCode: 403, body: JSON.stringify({ message: "User is not a member of the group." }) };
    }

    if (!group.usersConnected.includes(userId)) {
        return { statusCode: 403, body: JSON.stringify({ message: "User is not connected to the chat." }) };
    }

    // Append the new message to the group's messages array
    const updateResponse = await dynamoDb.update({
        TableName: groupsTableName,
        Key: { groupId },
        UpdateExpression: "SET messages = list_append(messages, :msg)",
        ExpressionAttributeValues: {
            ":msg": [{
                userId,
                message,
                timestamp: new Date().toISOString(),
            }],
        },
        ReturnValues: "UPDATED_NEW"
    }).promise();

    // After updating, fetch the updated messages array
    const updatedGroupResponse = await dynamoDb.get({
        TableName: groupsTableName,
        Key: { groupId },
    }).promise();
    const updatedMessages = updatedGroupResponse.Item.messages;

    // Retrieve all connections of users currently in the chat to broadcast the message
    const connectionData = await dynamoDb.scan({
        TableName: connectionsTableName,
        FilterExpression: "groupId = :groupId",
        ExpressionAttributeValues: {
            ":groupId": groupId,
        }        
    }).promise();

    const apiGatewayManagementApi = new AWS.ApiGatewayManagementApi({
        endpoint: process.env.WEBSOCKET_ENDPOINT
    });

    const broadcastPromises = connectionData.Items.map(async (item) => {

        try {
            await apiGatewayManagementApi.postToConnection({
                ConnectionId: item.connectionId,
                Data: JSON.stringify({
                    action: 'messageReceived',
                    messages: updatedMessages,
                    groupId
                })
            }).promise();
        } catch (error) {
            console.error(`Error sending message to connection: ${item.connectionId}`, error);

            // Handle stale connections by deleting them
            if (error.statusCode === 410) {
                console.log(`Deleting stale connection: ${item.connectionId}`);
                await dynamoDb.delete({
                    TableName: connectionsTableName,
                    Key: { connectionId: item.connectionId }
                }).promise();
            }
        }
    });

    await Promise.all(broadcastPromises);
    
    return { statusCode: 200, body: JSON.stringify({ message: "Message sent." }) };
};
