importScripts("https://www.gstatic.com/firebasejs/10.12.0/firebase-app-compat.js");
importScripts("https://www.gstatic.com/firebasejs/10.12.0/firebase-messaging-compat.js");

firebase.initializeApp({
  apiKey: "AIzaSyCCDoW8qZ70Wc6qw6WWgmH10Wl1KvZZWTA",
  authDomain: "emergency-ambulance-alert.firebaseapp.com",
  projectId: "emergency-ambulance-alert",
  storageBucket: "emergency-ambulance-alert.firebasestorage.app",
  messagingSenderId: "487464125337",
  appId: "1:487464125337:web:d69f15c29f32921b1f4908",
});

const messaging = firebase.messaging();

messaging.onBackgroundMessage((payload) => {
  self.registration.showNotification(payload.notification.title, {
    body: payload.notification.body
  });
});