import { Server } from "socket.io";

const messages = new Map();
const socketRooms = new Map();

export const connectToSocket = (server) => {
  const io = new Server(server, {
    cors: {
      origin: process.env.FRONTEND_URL || "*",
      methods: ["GET", "POST"],
      credentials: process.env.FRONTEND_URL ? true : false,
    },
  });

  io.on("connection", (socket) => {
    console.log("Connected:", socket.id);

    socket.on("join-call", async (path) => {
      if (!path || typeof path !== "string") return;

      // Prevent joining the same room multiple times
      const currentRoom = socketRooms.get(socket.id);

      if (currentRoom === path) {
        return;
      }

      // Leave previous call if the socket was already in one
      if (currentRoom) {
        socket.leave(currentRoom);
        socket.to(currentRoom).emit("user-left", socket.id);

        socketRooms.delete(socket.id);

        // Clean old messages if nobody is left
        const oldRoom = io.sockets.adapter.rooms.get(currentRoom);

        if (!oldRoom || oldRoom.size === 0) {
          messages.delete(currentRoom);
        }
      }

      // Join new room
      await socket.join(path);
      socketRooms.set(socket.id, path);

      // Get current users AFTER joining
      const room = io.sockets.adapter.rooms.get(path);
      const users = room ? [...room] : [];

      // Notify everyone in the room
      io.to(path).emit("user-joined", socket.id, users);

      // Send previous messages only to the new user
      const roomMessages = messages.get(path);

      if (roomMessages) {
        for (const message of roomMessages) {
          socket.emit(
            "chat-message",
            message.data,
            message.sender,
            message.socketIdSender,
          );
        }
      }
    });

    // WebRTC signaling
    socket.on("signal", (toId, message) => {
      if (!toId || !message) return;

      const room = socketRooms.get(socket.id);
      const targetSocket = io.sockets.sockets.get(toId);

      // Only allow signaling to someone in the same call
      if (!room || !targetSocket || socketRooms.get(toId) !== room) {
        return;
      }

      targetSocket.emit("signal", socket.id, message);
    });

    // Chat
    socket.on("chat-message", (data, sender) => {
      if (
        typeof data !== "string" ||
        typeof sender !== "string" ||
        data.length > 5000
      ) {
        return;
      }

      const room = socketRooms.get(socket.id);

      if (!room) return;

      if (!messages.has(room)) {
        messages.set(room, []);
      }

      messages.get(room).push({
        data,
        sender,
        socketIdSender: socket.id,
      });

      // Send to everyone in the call
      io.to(room).emit("chat-message", data, sender, socket.id);
    });

    // Disconnect
    socket.on("disconnect", () => {
      const room = socketRooms.get(socket.id);

      if (!room) {
        return;
      }

      socketRooms.delete(socket.id);

      // Notify remaining users
      socket.to(room).emit("user-left", socket.id);

      // Socket.IO automatically removes the socket from the room
      // before/around disconnect cleanup.
      setImmediate(() => {
        const remainingUsers = io.sockets.adapter.rooms.get(room);

        if (!remainingUsers || remainingUsers.size === 0) {
          messages.delete(room);
        }
      });

      console.log("Disconnected:", socket.id);
    });
  });

  return io;
};
