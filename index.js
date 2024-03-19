const AWS = require('aws-sdk');
const dynamoDb = new AWS.DynamoDB.DocumentClient();
const groupsTableName = process.env.GROUPS_TABLE;
const connectionsTableName = process.env.CONNECTIONS_TABLE;

exports.handler = async (event) => {
    const { groupId, userId, message } = JSON.parse(event.body);
    const connectionId = event.requestContext.connectionId;

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

    // Log the total number of connections to broadcast to
    console.log("Group:", group);
    console.log("Connectiondata:", connectionData);
    console.log(`Broadcasting message to ${connectionData.Items.length} connections for groupId: ${groupId}`);

    const broadcastPromises = connectionData.Items.map(async (item) => {
        // Log before attempting to post to each connection
        console.log(`Attempting to send message to connection: ${item.connectionId}`);

        try {
            await apiGatewayManagementApi.postToConnection({
                ConnectionId: item.connectionId,
                Data: JSON.stringify({
                    action: 'messageReceived',
                    message: {
                        userId,
                        content: message,
                        timestamp: new Date().toISOString(),
                    },
                    groupId
                })
            }).promise();

            // Log success after sending the message
            console.log(`Successfully sent message to connection: ${item.connectionId}`);
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

    // Log completion of the broadcasting process
    console.log(`Completed broadcasting messages for groupId: ${groupId}`);

    return { statusCode: 200, body: JSON.stringify({ message: "Message sent." }) };
};
