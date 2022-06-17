import mongoose from 'mongoose';

const genderOptions = {
  values: ['male', 'female'],
};

const userSchema = new mongoose.Schema(
  {
    name: {
      type: String,
      required: 'Name is required!',
      minlength: 3,
    },
    username: {
      type: String,
      required: 'Username is required!',
      minlength: 3,
      maxlength: 12,
      unique: true,
    },
    password: {
      type: String,
      required: 'Password is required!',
    },
    gender: {
      type: String,
      enum: genderOptions,
      required: 'Gender is required!',
    },
    address: {
      type: String,
      required: 'Address is required!',
    },
    status: {
      type: String,
      enum: ['active', 'disabled'],
      required: 'Status is required!',
      default: 'active',
    },
  },
  {
    timestamps: { createdAt: 'createdDate: ', updatedAt: 'modifyDate: ' },
    collection: 'users',
  },
);

export default mongoose.model('userModel', userSchema);
